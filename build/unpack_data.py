import os
import pandas as pd


def unpack_data(input_dir, output_file):
    """
    Unpacks and combines multiple CSV files from a directory into a single CSV file.

    Parameters:
    input_dir (str): Path to the directory containing the CSV files.
    output_file (str): Path to the output combined CSV file.
    """
    data_frames = []
    for file_name in os.listdir(input_dir):
        if file_name.endswith('.csv') or 'data-' in file_name:
            file_path = os.path.join(input_dir, file_name)
            data = pd.read_csv(
                file_path,
                names=['sequence', 'family_accession', 'sequence_name', 'aligned_sequence', 'family_id']
            )
            data_frames.append(data)
    combined_data = pd.concat(data_frames, ignore_index=True)
    combined_data.to_csv(output_file, index=False)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Unpack and combine protein data")
    parser.add_argument("--input_dir", type=str, required=True, help="Path to input directory")
    parser.add_argument("--output_file", type=str, required=True, help="Path to output combined CSV file")
    args = parser.parse_args()

    unpack_data(args.input_dir, args.output_file)
