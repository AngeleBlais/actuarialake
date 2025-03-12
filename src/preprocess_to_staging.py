import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
import numpy as np
import tqdm
import joblib
from collections import OrderedDict
import boto3
import io
import os
from io import StringIO
import json
from numba import njit

@njit
def split_indices(family_accession_encoded,unique_classes):
    print("Splitting data into train, dev, and test sets...")
    train_indices, dev_indices, test_indices = [], [], []

    for cls in unique_classes:
        class_data_indices = np.where(family_accession_encoded == cls)[0]
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
            np.random.seed(42)  
            np.random.shuffle(class_data_indices)
            train_split = int(len(class_data_indices) * (1 / 3))
            dev_split = train_split + int(len(class_data_indices) * (1 / 3))
            train_indices.extend(class_data_indices[:train_split])
            dev_indices.extend(class_data_indices[train_split:dev_split])
            test_indices.extend(class_data_indices[dev_split:])
            
    train_indices = np.array(train_indices)
    dev_indices = np.array(dev_indices)
    test_indices = np.array(test_indices)

    return train_indices, dev_indices, test_indices
    
def preprocess_data(client,bucket_raw,input_file,bucket_staging,output_prefix):
    """
    Preprocesses the raw data for model training and evaluation.

    Objectives:
    1. Load raw data from a CSV file.
    2. Clean the data (e.g., drop missing values).
    3. Encode categorical labels (`family_accession`) into integers.
    4. Split the data into train, dev, and test sets.
    5. Save preprocessed datasets and metadata.

    Steps:
    - Load the data with `pd.read_csv`.
    - Handle missing values using `dropna`.
    - Encode `family_accession` using `LabelEncoder`.
    - Split the data into train/dev/test using a manual approach.
    - Save the processed datasets (train.csv, dev.csv, test.csv) and class weights.

    Parameters:
    data_file (str): Path to the raw CSV file.
    output_dir (str): Directory to save the preprocessed files and metadata.
    """
    # Load the data
    print('Loading Data...')
    print(f"Downloading {input_file} from raw bucket...")
    response = client.get_object(Bucket=bucket_raw, Key=input_file)
    data = pd.read_csv(io.BytesIO(response['Body'].read())) 
    print(data.head(10))
    
    data = data.dropna()

    label_encoder = LabelEncoder()
    data['class_encoded'] = label_encoder.fit_transform(data['family_accession'])

    # Save label encoder mapping
    joblib.dump(label_encoder, f"./label_encoder.joblib")

    # Prepare label mapping
    label_mapping = dict(zip(label_encoder.classes_, map(int, label_encoder.transform(label_encoder.classes_))))
    json_data = json.dumps(label_mapping)

    s3.put_object(
            Bucket=bucket_staging,
            Key=f"label_mapping.txt", 
            Body=json_data  
    )

    unique_classes, class_counts = np.unique(data["class_encoded"], return_counts=True)

    # Manual train/dev/test split logic
    train_indices, dev_indices, test_indices = split_indices(data['class_encoded'].to_numpy(),unique_classes)

    # Use indices to create subsets of the original DataFrame
    train_data = data.iloc[train_indices]
    dev_data = data.iloc[dev_indices]
    test_data = data.iloc[test_indices]
    
    
    train_buf,dev_buf,test_buf = StringIO(),StringIO(),StringIO()
    train_data.to_csv(train_buf, index=False)
    dev_data.to_csv(dev_buf, index=False)
    test_data.to_csv(test_buf, index=False)
    
    train_buf.seek(0)
    dev_buf.seek(0)
    test_buf.seek(0)
    try:
        client.put_object(Bucket=bucket_staging, Body=train_buf.getvalue(), Key=f'train.csv')
        client.put_object(Bucket=bucket_staging, Body=dev_buf.getvalue(), Key=f'dev.csv')
        client.put_object(Bucket=bucket_staging, Body=test_buf.getvalue(), Key=f'test.csv')
        print("Files successfully uploaded to S3.")
    except Exception as e:
        print(f"Error uploading files: {e}")
    finally:
        # Fermer les buffers pour libérer la mémoire
        train_buf.close()
        dev_buf.close()
        test_buf.close()

    # Compute class weights and save them
    class_counts = train_data['class_encoded'].value_counts()
    class_weights = 1. / class_counts
    class_weights /= class_weights.sum()

    full_class_weights = {i: class_weights.get(i, 0.0) for i in range(max(class_counts.index) + 1)}

    
    json_data = json.dumps(full_class_weights)

    s3.put_object(
            Bucket=bucket_staging,
            Key=f"class_weights.txt", 
            Body=json_data  
    )




def init_client():
    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:4566")
    return s3

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Preprocess protein data")
    parser.add_argument("--bucket_raw", type=str, required=True, help="Path to raw CSV file")
    parser.add_argument("--input_file", type=str, required=True, help="Path to raw CSV file")
    parser.add_argument("--bucket_staging", type=str, required=True, help="Path to raw CSV file")
    parser.add_argument("--output_prefix", type=str, required=True, help="Directory to save preprocessed data")
    args = parser.parse_args()

    s3=init_client()
    preprocess_data(s3,args.bucket_raw,args.input_file,args.bucket_staging,args.output_prefix)

#######################################################
# Task: Modify this code to achieve the following:
# 1. Instead of reading the raw data from a local CSV file, download it from the `raw` bucket in LocalStack using the `boto3` library.
# 2. Upload the processed train, dev, and test splits to the `staging` bucket in LocalStack instead of saving them locally.
# 3. Accelerate the manual train/dev/test split logic by making it numpy only and compiling the code with numba.
#    I suggest you isolate the train/dev/test split code into a function, and use the @njit decorator from numba to compile it and then call in
#    in the preprocess function
# 4. Update the command-line arguments to:
#    - Replace `--data_file` with `--bucket_raw` and `--input_file` for S3 integration.
#    - Replace `--output_dir` with `--bucket_staging` and `--output_prefix` for S3 integration.

# Hints:
# - Use `boto3` to download raw data:
#   ```python
#   s3 = boto3.client(find the right parameters)
#   response = s3.get_object(Bucket=bucket_raw, Key=input_file)
#   data = pd.read_csv(io.BytesIO(response['Body'].read())) # Use this to read the data from the remote bucket.
#   ```