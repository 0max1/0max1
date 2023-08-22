"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import json

# Load JSON data from a file
def load_json(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

# Check for duplicate addresses in the JSON data
def check_for_duplicates(json_data, file_name):
    address_count = {}
    for item in json_data:
        address = item["address"]
        if address in address_count:
            address_count[address] += 1
        else:
            address_count[address] = 1

    duplicates = [address for address, count in address_count.items() if count > 1]

    if duplicates:
        print(f'Duplicate addresses found in {file_name}:')
        for dup in duplicates:
            print(f'\t{dup}')

# Compare two JSON files and find discrepancies
def compare_jsons(json1, json2):
    discrepancies = []

    dict1 = {item["address"]: item for item in json1}
    dict2 = {item["address"]: item for item in json2}

    all_addresses = set(dict1.keys()) | set(dict2.keys())

    for address in all_addresses:
        token1 = dict1.get(address)
        token2 = dict2.get(address)

        if not token1:
            discrepancies.append({"address": address, "status": "Missing in file1"})
            continue
        if not token2:
            discrepancies.append({"address": address, "status": "Missing in file2"})
            continue

        for key in token1:
            if token1[key] != token2[key]:
                discrepancies.append({
                    "address": address,
                    "key": key,
                    "value_in_file1": token1[key],
                    "value_in_file2": token2[key]
                })

    return discrepancies

if __name__ == '__main__':
    file1_path = 'file1.json'
    file2_path = 'file2.json'

    # Load JSON data from both files
    json1 = load_json(file1_path)
    json2 = load_json(file2_path)

    # Check for duplicates in each JSON data
    check_for_duplicates(json1, file1_path)
    check_for_duplicates(json2, file2_path)

    # Compare JSON files and find discrepancies
    differences = compare_jsons(json1, json2)

    # Print discrepancies or a message of consistency
    if differences:
        for diff in differences:
            if 'status' in diff:
                print(f'Token {diff["address"]} {diff["status"]}.')
            else:
                print(f'For token {diff["address"]}, key {diff["key"]}:')
                print(f'\tValue in {file1_path}: {diff["value_in_file1"]}')
                print(f'\tValue in {file2_path}: {diff["value_in_file2"]}')
    else:
        print('Both JSON files are consistent.')


