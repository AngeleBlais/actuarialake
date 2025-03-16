# Actuarialake
## 1 Introduction
En actuariat le flux de données est incessant et leur stockage en devient problématique. Le flux de données clients stockés dans le portefeuille ne cesse d’augmenter. Pour pallier ce problème de stockage, la mise en place d’un datalake s’impose. Pour ce faire, j'ai choisi une utilisation de buckets S3 via LocalStack. Cela permet de simuler un environnement cloud local à faible coût, tout en reproduisant les fonctionnalités d’AWS S3. Ainsi que tester et développer des processus de gestion de données sans avoir besoin d'une infrastructure cloud réelle, ce qui est plus rapide en phase de développement.

Par ailleurs ces derniers offrent un haut niveau de sécurité pour nos données. Les données sont chiffrées automatiquement lors du stockage et du transfert. S3 respecte des normes strictes comme le RGPD et la redondance des données garantit leur protection contre la perte. Contrairement aux services cloud externes, S3 donne un contrôle total sur la gestion des données.

## 2 Exigences de base
### 2.1 Architecture du Data Lake
Dataset: 3 sample générés aléatoirement en gardant la structure orginelle des bases de données portefeuilles, en utilisant la fonction np.random.randn() pour les données numériques et la fonction np.random.choice() pour les données catégorielles

### 2.2 Stockage
Data Lake structuré (Raw, Staging, Curated): Cette architecture en couches médail­lonnées assure une gestion optimisée et une récupération efficace des données à chaque étape du traitement. Elle permet de conserver les données sous leur forme brute (« schema-on-read ») avant de les nettoyer et de les transformer progressivement pour atteindre un niveau analytique avancé.

aws --endpoint-url=http://localhost:4566/ s3 mb s3://raw    Spécification d'un endpoint personnalisé et utilisation d'AWS CLI pour créer un bucket S3 nommé raw
aws --endpoint-url=http://localhost:4566/ s3 mb s3://staging
aws --endpoint-url=http://localhost:4566/ s3 mb s3://curated

Raw: Cette couche conserve les données dans leur état brut, ce qui permet de préserver l'intégrité des informations d'origine et de revenir aux données sources en cas de besoin
commande associée: python build/unpack_to_raw.py --input_dir /home/angele/workspace/Datalake/actuarialake/data/raw --bucket_name raw --output_file_name ./data/staging/combined_data.csv 

Staging: Elle permet de nettoyer et structurer les données avant qu'elles ne soient utilisées, assurant ainsi une meilleure qualité et une plus grande cohérence des informations pour les étapes suivantes
commande associée: python src/preprocess_to_staging.py     --bucket_raw raw     --bucket_staging staging     --input_file combined_data.csv     --output_prefix ./data/curated/curated_wdi.csv

Curated: Cette couche optimise les données pour les analyses et les applications tierces, en offrant des informations enrichies, performantes et prêtes à l'emploi pour la prise de décision ou l'intégration
commande associée: python src/process_to_curated.py     --bucket_staging staging     --bucket_curated curated     --input_file train.csv     --output_file gold_wdi.csv

### 2.3 Pipeline d’integration
Ces données étantsont utilisées dans le cadre de Recherche et Développement,. DVC est donc plus adapté que Airflow pour l'automatisation des données. Il est en effet conçu pour gérer les données et les modèles de machine learning.

DVC permet de versionner les fichiers CSV, de les stocker dans des espaces distants comme S3, et de garantir leur traçabilité à travers différentes versions. Contrairement à Airflow, qui se concentre sur l'automatisation de workflows complexes, DVC offre une solution plus légère. Les commandes sont plus simples pour ajouter, versionner et pousser des fichiers vers un stockage distant.

Configuration de DVC: dvc remote modify localstack-s3 access_key_id root
dvc remote modify localstack-s3 secret_access_key root
avec dvc repro la commande de lancement de dvc

### 2.4 API Gateway
FastAPI est utilisé pour exposer les données via une API REST, facilitant ainsi l'accès et l'interaction avec les données à travers différents points de terminaison (/raw, /staging, /curated)

Avec app.py le fichier python associé et uvicorn app:app (--reload) la commande de lancement 