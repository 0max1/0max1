"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import json
import psycopg2
from tqdm import tqdm
from web3 import Web3


def update_token_table(connection, token_data):
    with connection.cursor() as cursor:
        insert_content = ','.join(cursor.mogrify("(%s, %s, %s, %s)",
                                                 (token["address"], token.get("symbol", "")[:80],
                                                  int(token.get("decimal", 18)),
                                                  int(float((token.get("holder") or 0)))
                                                  )).decode('utf-8')
                                  for token in token_data)
        cursor.execute('INSERT INTO "Token" (token_address, token_symbol, decimal, num_holders)VALUES ' +
                       insert_content +
                       ' ON CONFLICT (token_address) DO UPDATE SET token_symbol = EXCLUDED.token_symbol, '
                       'decimal = EXCLUDED.decimal, num_holders = EXCLUDED.num_holders')
        connection.commit()
        print("Token data insertion/update complete.")


def update_pair_table(connection, holders_pair_flag):
    # This might be a faster way: create a temp table to accommodate those new tokens.
    # Inserting new pairs with joining, and then delete this temp table
    with connection.cursor() as cursor:
        getNewTokens = f"""
            SELECT T1.token_address, T2.token_address
            FROM "Token" T1 
            JOIN "Token" T2 ON T1.token_address < T2.token_address
            WHERE T1.num_holders >= {holders_pair_flag} AND T2.num_holders >= {holders_pair_flag} 
            AND (T1.is_new OR T2.is_new)
        """
        cursor.execute(getNewTokens)
        pairs = cursor.fetchall()
        print(f"{len(pairs)} new pairs is added")
        serialize = Web3()
        i = 0
        while i < len(pairs):
            insert_content = ','.join(
                cursor.mogrify("(%s)", (serialize.keccak(text=pair[0] + pair[1]).hex(),)).decode('utf-8') for pair in
                pairs[i: i + 1000000])
            cursor.execute('INSERT INTO "Pair" (pair_address) VALUES ' + insert_content + ' ON CONFLICT DO NOTHING')
            i += 1000000
        cursor.execute(f'UPDATE "Token" SET is_new = FALSE WHERE num_holders >= {holders_pair_flag}')
        connection.commit()
        print("Pair Table update complete.")


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
