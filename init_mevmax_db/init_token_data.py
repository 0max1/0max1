"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import json
import psycopg2


# def create_connection(user, password, host, database):
#     try:
#         connection = psycopg2.connect(
#             user=user,
#             password=password,
#             host=host,
#             dbname=database
#         )
#         return connection
#     except (Exception, psycopg2.Error) as error:
#         print("Error while connecting to PostgreSQL", error)
#         return None

def clear_token_table(connection):
    """
    Clears the "Token" table in the database.

    Args:
        connection (psycopg2.extensions.connection): The database connection object.
    """
    try:
        cursor = connection.cursor()
        cursor.execute('DELETE FROM "Token"')
        connection.commit()
        cursor.close()
        print("Token table cleared.")
    except (Exception, psycopg2.Error) as error:
        print("Error while clearing Token table", error)


def read_token_data(token_data_path):
    """
    Reads token data from a JSON file.

    Args:
        token_data_path (str): Path to the JSON file containing token data.

    Returns:
        list: List of token data.
    """
    with open(token_data_path, "r") as f:
        data = json.load(f)
    return data


def insert_token_table(connection, token_data):
    """
    Inserts or updates token data into the "Token" table.

    Args:
        connection (psycopg2.extensions.connection): The database connection object.
        token_data (list): List of token data.
    """
    cursor = connection.cursor()

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

            connection.commit()
            print("Token data insertion/update complete.")
        except (Exception, psycopg2.Error) as error:
            print("Error while inserting/updating Token data", error)

    cursor.close()


# if __name__ == "__main__":
#     user = "postgres",
#     password = "1106",
#     host = "localhost",
#     database = "MevMax"
#     connection = create_connection(user, password, host, database)
#     if connection:
#         clear_token_table(connection)
#         token_data_path = ""
#         token_data = read_token_data(token_data_path)
#         insert_token_table(connection, token_data)
#         connection.close()