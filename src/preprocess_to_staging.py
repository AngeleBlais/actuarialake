import pandas as pd
from sklearn.preprocessing import LabelEncoder
import numpy as np
import joblib
import boto3
import io
from io import StringIO
import json
from numba import njit

@njit
def split_indices(family_accession_encoded, unique_classes):
    # Using a round-robin scheme for classes with a single instance
    train_indices = []
    dev_indices = []
    test_indices = []
    counter = 0  # Counter to rotate assignment for single instance classes

    for i in range(unique_classes.shape[0]):
        cls = unique_classes[i]
        # Get the positions in the array where the class matches cls
        class_data_indices = np.where(family_accession_encoded == cls)[0]
        count = class_data_indices.shape[0]

        if count == 1:
            # Round-robin assignment for single instance classes
            if counter % 3 == 0:
                for j in range(count):
                    train_indices.append(class_data_indices[j])
            elif counter % 3 == 1:
                for j in range(count):
                    dev_indices.append(class_data_indices[j])
            else:
                for j in range(count):
                    test_indices.append(class_data_indices[j])
            counter += 1

        elif count == 2:
            # For two instances, assign first to train and second to test
            train_indices.append(class_data_indices[0])
            test_indices.append(class_data_indices[1])
        elif count == 3:
            # For three instances, assign one each to train, dev, test
            train_indices.append(class_data_indices[0])
            dev_indices.append(class_data_indices[1])
            test_indices.append(class_data_indices[2])
        else:
            # For more than 3 instances, shuffle and split approximately 1/3 each
            np.random.seed(42)
            np.random.shuffle(class_data_indices)
            train_split = int(count * (1/3))
            dev_split = train_split + int(count * (1/3))
            for j in range(train_split):
                train_indices.append(class_data_indices[j])
            for j in range(train_split, dev_split):
                dev_indices.append(class_data_indices[j])
            for j in range(dev_split, count):
                test_indices.append(class_data_indices[j])
    
    return np.array(train_indices), np.array(dev_indices), np.array(test_indices)
    
def preprocess_data(client, bucket_raw, input_file, bucket_staging, output_prefix):
    """
    Preprocesses the raw data for model training and evaluation:
      1. Downloads raw CSV data from the S3 raw bucket.
      2. Drops rows with missing 'cout_tot' values.
      3. Encodes the `cod_hex` column into integers.
      4. Splits the data into train, dev, and test sets using a numpy-based split (compiled with numba).
      5. Uploads the resulting CSV splits and metadata to the staging bucket.
    """
    # Load the data from S3
    print('Loading Data...')
    print(f"Downloading {input_file} from raw bucket...")
    response = client.get_object(Bucket=bucket_raw, Key=input_file)
    data = pd.read_csv(io.BytesIO(response['Body'].read()))
    print("Raw data head:")
    print(data.head(10))
    
    # Drop rows where "cout_tot" is NaN
    data = data.dropna(subset=["cout_tot"])
    print("After dropping rows with missing 'cout_tot':", data.shape)

    # Encode labels from the column 'cod_hex'
    label_encoder = LabelEncoder()
    data['class_encoded'] = label_encoder.fit_transform(data['cod_hex'])

    # Save label encoder mapping locally and upload to staging bucket
    joblib.dump(label_encoder, "./label_encoder.joblib")
    label_mapping = dict(zip(label_encoder.classes_, label_encoder.transform(label_encoder.classes_).tolist()))
    json_data = json.dumps(label_mapping)
    client.put_object(
        Bucket=bucket_staging,
        Key="label_mapping.txt", 
        Body=json_data  
    )

    # Get unique classes from the encoded labels
    unique_classes = np.unique(data["class_encoded"])

    # Manual train/dev/test split using numba compiled function
    train_indices, dev_indices, test_indices = split_indices(data['class_encoded'].to_numpy(), unique_classes)

    # Create subsets of the original DataFrame using positional indexing
    train_data = data.iloc[train_indices]
    dev_data = data.iloc[dev_indices]
    test_data = data.iloc[test_indices]
    
    print("Train Data:")
    print(train_data.head(10))
    print("Dev Data:")
    print(dev_data.head(10))
    print("Test Data:")
    print(test_data.head(10))

    # Upload CSV splits to the staging bucket
    train_buf, dev_buf, test_buf = StringIO(), StringIO(), StringIO()
    train_data.to_csv(train_buf, index=False)
    dev_data.to_csv(dev_buf, index=False)
    test_data.to_csv(test_buf, index=False)
    
    train_buf.seek(0)
    dev_buf.seek(0)
    test_buf.seek(0)
    try:
        client.put_object(Bucket=bucket_staging, Body=train_buf.getvalue(), Key='train.csv')
        client.put_object(Bucket=bucket_staging, Body=dev_buf.getvalue(), Key='dev.csv')
        client.put_object(Bucket=bucket_staging, Body=test_buf.getvalue(), Key='test.csv')
        print("Files successfully uploaded to S3.")
    except Exception as e:
        print(f"Error uploading files: {e}")
    finally:
        train_buf.close()
        dev_buf.close()
        test_buf.close()

    # Compute class weights and save them only if training data is available
    class_counts = train_data['class_encoded'].value_counts()
    if class_counts.empty:
        print("Warning: No training data available. Skipping class weights calculation.")
    else:
        class_weights = 1. / class_counts
        class_weights /= class_weights.sum()
        if len(class_counts.index) > 0:
            full_class_weights = {i: class_weights.get(i, 0.0) for i in range(max(class_counts.index) + 1)}
        else:
            full_class_weights = {}
        json_data = json.dumps(full_class_weights)
        client.put_object(
            Bucket=bucket_staging,
            Key="class_weights.txt", 
            Body=json_data  
        )
        print("Class weights uploaded successfully.")

def init_client():
    client = boto3.client(
        "s3",
        endpoint_url="http://localhost:4566"
    )
    return client

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Preprocess protein data")
    parser.add_argument("--bucket_raw", type=str, required=True, help="Raw bucket name")
    parser.add_argument("--input_file", type=str, required=True, help="Input CSV file name in raw bucket")
    parser.add_argument("--bucket_staging", type=str, required=True, help="Staging bucket name")
    parser.add_argument("--output_prefix", type=str, required=True, help="Output prefix (unused in current script)")
    args = parser.parse_args()

    client = init_client()
    preprocess_data(client, args.bucket_raw, args.input_file, args.bucket_staging, args.output_prefix)
