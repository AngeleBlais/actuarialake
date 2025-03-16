from fastapi import FastAPI, HTTPException
import boto3
import os

# Initialisation de l'application FastAPI
app = FastAPI()

# Initialiser le client S3 (LocalStack ou AWS)
s3_client = boto3.client(
    's3',
    aws_access_key_id='root',
    aws_secret_access_key='root',
    endpoint_url="http://localhost:4566",  # Pour LocalStack
)

# Nom des buckets
raw_bucket = 'raw'
staging_bucket = 'staging'
curated_bucket = 'curated'

# Point de terminaison racine
@app.get("/")
def read_root():
    return {"message": "Welcome to the Data API"}

# Point de terminaison pour les données brutes
@app.get("/raw")
async def get_raw_data():
    try:
        response = s3_client.list_objects_v2(Bucket=raw_bucket)
        if 'Contents' in response:
            return {"raw_data": [obj['Key'] for obj in response['Contents']]}
        else:
            raise HTTPException(status_code=404, detail="No raw data found.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Point de terminaison pour les données intermédiaires (staging)
@app.get("/staging")
async def get_staging_data():
    try:
        response = s3_client.list_objects_v2(Bucket=staging_bucket)
        if 'Contents' in response:
            return {"staging_data": [obj['Key'] for obj in response['Contents']]}
        else:
            raise HTTPException(status_code=404, detail="No staging data found.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Point de terminaison pour les données finales (curated)
@app.get("/curated")
async def get_curated_data():
    try:
        response = s3_client.list_objects_v2(Bucket=curated_bucket)
        if 'Contents' in response:
            return {"curated_data": [obj['Key'] for obj in response['Contents']]}
        else:
            raise HTTPException(status_code=404, detail="No curated data found.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def check_bucket(bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return "up"
    except Exception:
        return "down"

@app.get("/health")
async def health_check():
    bucket_statuses = {name: check_bucket(bucket) for name, bucket in [raw_bucket,staging_bucket,curated_bucket].items()}
    
    # Vérifie si un bucket est en panne
    overall_status = "healthy" if all(status == "up" for status in bucket_statuses.values()) else "degraded"
    
    return {
        "status": overall_status,
        "buckets": bucket_statuses
    }
@app.get("/stats")
async def stats():
    try:
        # Obtient les informations des objets dans les buckets
        raw_stats = s3_client.list_objects_v2(Bucket=raw_bucket)
        staging_stats = s3_client.list_objects_v2(Bucket=staging_bucket)
        curated_stats = s3_client.list_objects_v2(Bucket=curated_bucket)
        
        # Retourne les statistiques sur le nombre d'objets et leur taille dans chaque bucket
        return {
            "raw_bucket": {
                "object_count": len(raw_stats.get('Contents', [])),
                "size": sum(obj['Size'] for obj in raw_stats.get('Contents', []))
            },
            "staging_bucket": {
                "object_count": len(staging_stats.get('Contents', [])),
                "size": sum(obj['Size'] for obj in staging_stats.get('Contents', []))
            },
            "curated_bucket": {
                "object_count": len(curated_stats.get('Contents', [])),
                "size": sum(obj['Size'] for obj in curated_stats.get('Contents', []))
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Gestionnaire pour le favicon.ico (optionnel)
@app.get("/favicon.ico")
async def favicon():
    return {"message": "No favicon provided"}
