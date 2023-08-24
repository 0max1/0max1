"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import json
import psycopg2
from tqdm import tqdm


# def create_connection(user, password, host, database):
#     try:
#         connection = psycopg2.connect(
#             user,
#             password,
#             host,
#             database
#         )
#         return connection
#     except (Exception, psycopg2.Error) as error:
#         print("Error while connecting to PostgreSQL", error)
#         return None

def read_token_data(token_data_path):
    with open(token_data_path, "r") as f:
        data = json.load(f)
    return data


def update_token_table(connection, token_data, holders_pair_flag):
    try:
        cursor = connection.cursor()

        new_token_addresses = set()  # To store newly inserted token addresses

        for token in token_data:
            try:
                token_symbol = token.get("symbol", "")[:80]
                token_address = token["address"]
                num_holders = int(token.get("holder", 0))
                decimals = int(token.get("decimal", 18))  # Default decimal value is 18

                cursor.execute('SELECT * FROM "Token" WHERE token_address = %s', (token_address,))
                existing_token = cursor.fetchone()

                if existing_token:
                    existing_symbol = existing_token[1]
                    existing_decimal = existing_token[3]
                    existing_num_holders = existing_token[4]

                    if existing_symbol != token_symbol or existing_decimal != decimals or existing_num_holders != num_holders:
                        cursor.execute(
                            'UPDATE "Token" SET token_symbol = %s, decimal = %s, num_holders = %s WHERE token_address = %s',
                            (token_symbol, decimals, num_holders, token_address))
                        print(f"Token with address {token_address} updated.")
                else:
                    cursor.execute(
                        'INSERT INTO "Token" (token_symbol, token_address, decimal, num_holders) VALUES (%s, %s, %s, %s)',
                        (token_symbol, token_address, decimals, num_holders))
                    print(f"New token with address {token_address} inserted.")
                    if num_holders >= holders_pair_flag:
                        new_token_addresses.add(token_address)  # Add new token address to the set

                connection.commit()
                print("Token data insertion/update complete.")
            except (Exception, psycopg2.Error) as inner_error:
                print(f"Error while inserting/updating Token data for token {token_address}", inner_error)

        cursor.close()
        return new_token_addresses  # Return the set of new token addresses
    except (Exception, psycopg2.Error) as outer_error:
        print("Error while inserting/updating Token data", outer_error)

def update_pair_table(connection, new_token_addresses, holders_pair_flag):
    try:
        cursor = connection.cursor()

        # Get existing token IDs from the Token table
        cursor.execute('SELECT token_id, token_address FROM "Token" WHERE num_holders >= %s', (holders_pair_flag,))
        token_data = cursor.fetchall()
        token_id_by_address = {row[1]: row[0] for row in token_data}

        # Calculate the total number of new combinations
        total_combinations = (len(new_token_addresses) * len(token_data)) - len(new_token_addresses)

        pairs = set()
        with tqdm(total=total_combinations, desc="Updating Pair Table", unit="pair") as pbar:
            for i, token1_address in enumerate(new_token_addresses):
                token1_id = token_id_by_address[token1_address]

                for token2_data in token_data:
                    token2_id = token2_data[0]
                    token2_address = token2_data[1]

                    if token1_address == token2_address:
                        continue  # Skip pairing with itself

                    # Ensure the combination is unique and insert into Pair table
                    pair = tuple(sorted([token1_id, token2_id]))
                    if pair not in pairs:
                        # Check if the pair exists in Pair table
                        cursor.execute('SELECT pair_id FROM "Pair" WHERE token1_id = %s AND token2_id = %s', (pair[0], pair[1]))
                        existing_pair = cursor.fetchone()

                        if existing_pair:
                            pair_id = existing_pair[0]
                        else:
                            # Insert a new row into Pair table if the pair doesn't exist
                            cursor.execute('INSERT INTO "Pair" (token1_id, token2_id) VALUES (%s, %s)', pair)
                            connection.commit()

                            # Get the newly inserted pair_id
                            cursor.execute('SELECT lastval()')
                            pair_id = cursor.fetchone()[0]

                        pairs.add(pair)
                        pbar.update(1)

        cursor.close()
        connection.commit()
        print("Pair Table update complete.")
    except (Exception, psycopg2.Error) as error:
        print("Error while updating Pair Table:", error)

# if __name__ == "__main__":
#     user = "postgres",
#     password = "1106",
#     host = "localhost",
#     database = "MevMax"
#     connection = create_connection(user, password, host, database)
#     if connection:
#         token_data_path = ""
#         token_data = read_token_data(token_data_path)
#         new_token_addresses = update_token_table(connection, token_data)
#         update_pair_table(connection, new_token_addresses)
#         connection.close()