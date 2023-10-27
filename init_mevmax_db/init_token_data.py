"""
Author: Justin Jia, Zhenhao Lu
Last Updated: October 14, 2023
Version: 1.0.3
"""

import json
import psycopg2


# def clear_token_table(connection):
#     """
#     Clears the "Token" table in the database.
#
#     Args:
#         connection (psycopg2.extensions.connection): The database connection object.
#     """
#     try:
#         cursor = connection.cursor()
#         cursor.execute('DELETE FROM "Token"')
#         connection.commit()
#         cursor.close()
#         print("Token table cleared.")
#     except (Exception, psycopg2.Error) as error:
#         print("Error while clearing Token table", error)


def read_json_data(data_path):
    """
        Reads token/pool data from a JSON file.

        Args:
            data_path (str): Path to the JSON file containing token/pool data.

        Returns:
            list: List of token/pool data.
        """
    with open(data_path, "r") as f:
        data = json.load(f)
    return data


# def read_token_data(token_data_path):
#     """
#     Reads token data from a JSON file.
#
#     Args:
#         token_data_path (str): Path to the JSON file containing token data.
#
#     Returns:
#         list: List of token data.
#     """
#     with open(token_data_path, "r") as f:
#         data = json.load(f)
#     return data


def insert_token_table(connection, token_data):
    """
    Inserts or updates token data into the "Token" table.

    Args:
        connection (psycopg2.extensions.connection): The database connection object.
        token_data (list): List of token data.
            Eg: [{"token_address": "address1", "token_symbol": "symbol1", "decimal": 1, "holder": 1},
            {"token_address": "address2", ...}, ...]

    Default Values: decimal = 18, num_holder = 0, token_symbol = ''
    In the latest database structure, token_symbol will not be recorded any more though this column will not be removed
    """
    with connection.cursor() as cursor:
        args = ','.join(cursor.mogrify("(%s, %s, %s, %s)",
                                       (token["address"], "",
                                        int(token.get("decimal", 18)),
                                        int(float((token.get("holder") or 0)))
                                        )).decode('utf-8')
                        for token in token_data)
        cursor.execute('INSERT INTO "Token" VALUES ' + args)
        connection.commit()
        print("Token Table Initialization")

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
