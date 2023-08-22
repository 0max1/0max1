"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import os

import pandas as pd
import json

CAL_ROUTE_DIR = os.path.dirname(os.path.abspath(__file__))

def pool_convert_csv_to_json(csv_file, output_folder, CAL_DIR):
    # Ensure the output folder exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Read CSV data into a DataFrame
    df = pd.read_csv(csv_file)

    # Create a list to store the JSON objects
    json_data = []

    # Loop through each row in the DataFrame and convert to JSON
    for _, row in df.iterrows():
        pool_data = {
            "Name": row["Name"],
            "pool_address": row["pool_address"],
            "tvl": row["tvl"],
            "fee": row["fee"]
        }

        for i in range(1, 9):
            token_address_key = f"token{i}"
            token_symbol_key = f"token{i}_symbol"
            token_decimals_key = f"token{i}_decimals"

            if token_address_key in row:
                token_address = row[token_address_key]
                token_symbol = row[token_symbol_key]
                token_decimals = row[token_decimals_key]

                if pd.notna(token_address):
                    pool_data[token_address_key] = token_address
                    pool_data[token_symbol_key] = token_symbol
                    pool_data[token_decimals_key] = int(token_decimals)

        json_data.append(pool_data)

    # Convert the list of JSON objects to JSON format
    json_str = json.dumps(json_data, indent=2)

    # Write the JSON data to a file
    output_path = os.path.join(CAL_DIR, output_folder, "bsc_pool.json")
    with open(output_path, "w") as json_file:
        json_file.write(json_str)

    print("CSV data converted to JSON successfully!")


def token_convert_csv_to_json(csv_file, output_folder, CAL_DIR):
    # Ensure the output folder exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Read CSV data into a DataFrame
    df = pd.read_csv(csv_file)

    # Create a list to store the JSON objects
    json_data = []

    # Loop through each row in the DataFrame and convert to JSON
    for _, row in df.iterrows():
        token_data = {
            "address": row["token"],
            "symbol": row["symbol"],
            "decimal": int(row["decimals"]),
            "holder": int(row["holder"])
        }

        json_data.append(token_data)

    # Convert the list of JSON objects to JSON format
    json_str = json.dumps(json_data, indent=2)

    # Write the JSON data to a file
    output_path = os.path.join(CAL_DIR, output_folder, "bsc_token.json")
    with open(output_path, "w") as json_file:
        json_file.write(json_str)

    print("CSV data converted to JSON successfully!")


if __name__ == "__main__":
    output_directory = "original_data"
    CAL_DIR = os.path.dirname(os.path.abspath(__file__))
    pool_convert_csv_to_json("bsc_pool.csv", output_directory, CAL_DIR)
    token_convert_csv_to_json("bsc_token.csv", output_directory, CAL_DIR)