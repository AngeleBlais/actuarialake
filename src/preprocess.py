import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
import numpy as np
import tqdm
import joblib
from collections import OrderedDict


def preprocess_data(data_file, output_dir):
    """
    Exercice : Fonction pour prétraiter les données brutes et les préparer pour l'entraînement de modèles.

    Objectifs :
    1. Charger les données brutes à partir d’un fichier CSV.
    2. Nettoyer les données (par ex. : supprimer les valeurs manquantes).
    3. Encoder les labels catégoriels (colonne `family_accession`) en entiers.
    4. Diviser les données en ensembles d’entraînement, de validation et de test selon une logique définie.
    5. Sauvegarder les ensembles prétraités et des métadonnées utiles.

    Étapes :
    - Charger les données avec `pd.read_csv`.
    - Supprimer les valeurs manquantes avec `dropna`.
    - Encoder les valeurs de `family_accession` en utilisant `LabelEncoder`.
    - Diviser les données en ensembles d’entraînement, de validation et de test.
    - Sauvegarder les données prétraitées en fichiers CSV (train.csv, dev.csv, test.csv).
    - Calculer et sauvegarder les poids de classes pour équilibrer les classes.

    Paramètres :
    - data_file (str) : Chemin vers le fichier CSV contenant les données brutes.
    - output_dir (str) : Répertoire où les fichiers prétraités et les métadonnées seront sauvegardés.

    Indices :
    - Utilisez `LabelEncoder` pour encoder les catégories.
    - Utilisez `train_test_split` pour diviser les indices des données.
    - Utilisez `to_csv` pour sauvegarder les fichiers prétraités.
    - Calculez les poids de classes en utilisant les comptes des classes.

    Défis bonus :
    - Assurez que les données très déséquilibrées sont bien réparties dans les ensembles.
    - Générez des fichiers supplémentaires comme un mapping des classes et les poids de classes.
    """
    # Load the data
    print('Loading Data')
    data = pd.read_csv(data_file)

    # Handle missing values
    data = data.dropna()

    # Encode the family_accession to numeric labels
    label_encoder = LabelEncoder()
    data['class_encoded'] = label_encoder.fit_transform(data['family_accession'])

    # Save the label encoder
    joblib.dump(label_encoder, f'{output_dir}/label_encoder.joblib')

    # Save the label encoder mapping before removing classes
    label_mapping = dict(zip(label_encoder.classes_, label_encoder.transform(label_encoder.classes_)))
    with open(f'{output_dir}/label_mapping.txt', 'w') as f:
        for key, value in label_mapping.items():
            f.write(f"{key}: {value}\n")

    # Convert to numpy arrays for faster processing
    family_accession = data['family_accession'].values
    class_encoded = data['class_encoded'].values

    unique_classes, class_counts = np.unique(family_accession, return_counts=True)

    train_indices = []
    dev_indices = []
    test_indices = []

    # Distribute classes with specific logic
    print("Distributing data")
    for cls in tqdm.tqdm(unique_classes):
        class_data_indices = np.where(family_accession == cls)[0]
        count = len(class_data_indices)

        if count == 1:
            test_indices.extend(class_data_indices)
        elif count == 2:
            dev_indices.extend(class_data_indices[:1])
            test_indices.extend(class_data_indices[1:])
        elif count == 3:
            train_indices.extend(class_data_indices[:1])
            dev_indices.extend(class_data_indices[1:2])
            test_indices.extend(class_data_indices[2:])
        else:
            train_part, remaining = train_test_split(
                class_data_indices,
                test_size=2/3,
                random_state=42,
                stratify=class_encoded[class_data_indices]
            )
            dev_part, test_part = train_test_split(
                remaining,
                test_size=0.5,
                random_state=42,
                stratify=class_encoded[remaining]
            )
            train_indices.extend(train_part)
            dev_indices.extend(dev_part)
            test_indices.extend(test_part)

    # Convert indices lists to numpy arrays
    train_indices = np.array(train_indices)
    dev_indices = np.array(dev_indices)
    test_indices = np.array(test_indices)

    # Create DataFrames from the indices
    train_data = data.iloc[train_indices]
    dev_data = data.iloc[dev_indices]
    test_data = data.iloc[test_indices]

    # Drop columns not useful for prediction
    train_data = train_data.drop(columns=["family_id", "sequence_name", "family_accession"])
    dev_data = dev_data.drop(columns=["family_id", "sequence_name", "family_accession"])
    test_data = test_data.drop(columns=["family_id", "sequence_name", "family_accession"])

    # Save the preprocessed data
    train_data.to_csv(f'{output_dir}/train.csv', index=False)
    dev_data.to_csv(f'{output_dir}/dev.csv', index=False)
    test_data.to_csv(f'{output_dir}/test.csv', index=False)

    # Determine class weights
    class_counts = train_data['class_encoded'].value_counts()
    class_weights = 1. / class_counts
    class_weights /= class_weights.sum()

    # Calculate the scaling factor
    min_weight = class_weights.max()
    weight_scaling_factor = 1 / min_weight

    # Apply the scaling factor to class weights
    class_weights *= weight_scaling_factor

    # Create a full range of class weights with missing classes having weight 0
    max_class = max(class_counts.index)
    full_class_weights = {i: class_weights.get(i, 0.0) for i in range(max_class + 1)}

    # Save the class weights
    class_weights_dict = OrderedDict(sorted(full_class_weights.items()))
    with open(f'{output_dir}/class_weights.txt', 'w') as f:
        for key, value in class_weights_dict.items():
            f.write(f"{key}: {value}\n")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Preprocess protein data")
    parser.add_argument("--data_file", type=str, required=True, help="Path to train CSV file")
    parser.add_argument("--output_dir", type=str, required=True, help="Directory to save the preprocessed files")
    args = parser.parse_args()

    preprocess_data(args.data_file, args.output_dir)
