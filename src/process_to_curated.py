import io
import pandas as pd
import boto3
import numpy as np
from sklearn.preprocessing import StandardScaler

def curate_data(bucket_staging, bucket_curated, input_file, output_file):
    """
    Transforme les données issues du bucket staging en un jeu de données "curated" en appliquant
    une ingénierie de caractéristiques adaptée aux données (par exemple, de type actuariat).

    Étapes :
      1. Télécharger le fichier depuis le bucket de staging.
      2. Imputer les valeurs manquantes pour les colonnes numériques.
      3. Supprimer la colonne 'Tarification' si elle est entièrement vide.
      4. Calculer l'indicateur 'cost_ratio' = cout_tot / prime (en évitant la division par zéro).
      5. Normaliser certaines variables numériques clés.
      6. Construire une variable 'risk_score' en combinant cost_ratio (50%), WDI_Inflation_rate (30%)
         et N_Inondation (20%).
      7. Sauvegarder localement le fichier final et le téléverser dans le bucket curated.

    Paramètres :
      bucket_staging (str): Nom du bucket S3 de staging.
      bucket_curated (str): Nom du bucket S3 curated.
      input_file (str): Nom du fichier d'entrée dans le bucket de staging.
      output_file (str): Nom du fichier de sortie dans le bucket curated.
    """
    # Initialisation du client S3
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')
    
    # Étape 1 : Télécharger les données
    print(f"Téléchargement de {input_file} depuis le bucket de staging...")
    response = s3.get_object(Bucket=bucket_staging, Key=input_file)
    data = pd.read_csv(io.BytesIO(response['Body'].read()))
    print("Aperçu des données staging :")
    print(data.head())
    
    # Étape 2 : Imputation des valeurs manquantes pour les colonnes numériques
    numeric_cols = data.select_dtypes(include=[np.number]).columns.tolist()
    # Conserver uniquement les colonnes numériques ayant au moins une valeur non nulle
    numeric_cols = [col for col in numeric_cols if data[col].notna().sum() > 0]
    for col in numeric_cols:
        median_val = data[col].median()
        # Si la médiane est NaN, on remplace par 0
        data.loc[:, col] = data[col].fillna(median_val if not pd.isna(median_val) else 0)
    
    # Étape 3 : Supprimer la colonne 'Tarification' si elle est entièrement vide
    if 'Tarification' in data.columns and data['Tarification'].isna().all():
        data.drop(columns=['Tarification'], inplace=True)
    
    # Étape 4 : Calculer l'indicateur cost_ratio
    data["cost_ratio"] = data.apply(lambda row: row["cout_tot"] / row["prime"] if row["prime"] != 0 else 0, axis=1)
    
    # Étape 5 : Normalisation des variables numériques clés
    scaler = StandardScaler()
    cols_to_scale = ["cout_tot", "prime", "N_Inondation", "WDI_GDP_per_capita", "WDI_Inflation_rate", "cost_ratio"]
    cols_to_scale = [col for col in cols_to_scale if col in data.columns]
    
    data_scaled = data.copy()
    for col in cols_to_scale:
        # Si la colonne a plus d'une valeur unique, on la normalise
        if data[col].nunique() > 1:
            data_scaled[col] = scaler.fit_transform(data[[col]])
        else:
            data_scaled[col] = 0.0  # sinon, on attribue 0
    
    # Étape 6 : Calculer le risk_score (moyenne pondérée)
    risk_score = np.zeros(len(data_scaled))
    weight_total = 0
    if "cost_ratio" in data_scaled.columns:
        risk_score += 0.5 * data_scaled["cost_ratio"]
        weight_total += 0.5
    if "WDI_Inflation_rate" in data_scaled.columns:
        risk_score += 0.3 * data_scaled["WDI_Inflation_rate"]
        weight_total += 0.3
    if "N_Inondation" in data_scaled.columns:
        risk_score += 0.2 * data_scaled["N_Inondation"]
        weight_total += 0.2
    data_scaled["risk_score"] = risk_score / weight_total if weight_total != 0 else 0
    
    # Étape 7 : Sauvegarder localement le jeu de données curaté et le téléverser dans le bucket curated
    local_output_path = f"/tmp/{output_file}"
    data_scaled.to_csv(local_output_path, index=False)
    print(f"Jeu de données curaté sauvegardé localement à {local_output_path}.")
    
    print(f"Téléversement de {output_file} dans le bucket curated...")
    with open(local_output_path, "rb") as f:
        s3.upload_fileobj(f, bucket_curated, output_file)
    
    print(f"Jeu de données curaté téléversé avec succès dans le bucket '{bucket_curated}' sous le nom '{output_file}'.")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Curate data from staging to curated bucket using feature engineering")
    parser.add_argument("--bucket_staging", type=str, required=True, help="Nom du bucket S3 de staging")
    parser.add_argument("--bucket_curated", type=str, required=True, help="Nom du bucket S3 curated")
    parser.add_argument("--input_file", type=str, required=True, help="Nom du fichier d'entrée dans le bucket de staging")
    parser.add_argument("--output_file", type=str, required=True, help="Nom du fichier de sortie dans le bucket curated")
    args = parser.parse_args()

    curate_data(args.bucket_staging, args.bucket_curated, args.input_file, args.output_file)
