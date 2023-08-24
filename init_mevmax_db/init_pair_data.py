"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""
import threading

import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from tqdm import tqdm

# def create_connection():
#     try:
#         connection = psycopg2.connect(
#             user="postgres",
#             password="1106",
#             host="localhost",
#             database="MevMax"
#         )
#         return connection
#     except (Exception, psycopg2.Error) as error:
#         print("Error while connecting to PostgreSQL", error)
#         return None

def clear_pair_table(connection):
    """
    Clears the "Pair" table and related rows in the "Pool_Pair" table.

    Args:
        connection (psycopg2.extensions.connection): The database connection object.
    """
    try:
        cursor = connection.cursor()
        cursor.execute('DELETE FROM "Pool_Pair" WHERE pair_id IN (SELECT pair_id FROM "Pair")')
        cursor.execute('DELETE FROM "Pair"')
        connection.commit()
        print("Pair Table and related Pool_Pair rows cleared.")
        cursor.close()
    except (Exception, psycopg2.Error) as error:
        print("Error while clearing Pair Table and related Pool_Pair rows", error)

def insert_pair_table(connection, holders_pair_flag):
    """
    Inserts pairs of tokens into the "Pair" table.

    Args:
        connection (psycopg2.extensions.connection): The database connection object.
    """
    try:
        cursor = connection.cursor()

        # Get all token IDs from the Token table
        cursor.execute('SELECT token_id, token_address FROM "Token" WHERE num_holders >= %s', (holders_pair_flag,))
        token_ids = [row[0] for row in cursor.fetchall()]

        # Calculate the total number of combinations
        total_combinations = (len(token_ids) * (len(token_ids) - 1)) // 2

        pairs = set()
        with tqdm(total=total_combinations, desc="Init Pair Table", unit="pair") as pbar:
            for i in range(len(token_ids)):
                for j in range(i + 1, len(token_ids)):
                    token1_id = token_ids[i]
                    token2_id = token_ids[j]

                    # Ensure the combination is unique and insert into Pair table
                    pair = tuple(sorted([token1_id, token2_id]))
                    if pair not in pairs:
                        # Check if the pair exists in Pair table
                        cursor.execute('SELECT pair_id FROM "Pair" WHERE token0_id = %s AND token1_id = %s', (pair[0], pair[1]))
                        existing_pair = cursor.fetchone()

                        if existing_pair:
                            pair_id = existing_pair[0]
                        else:
                            # Insert a new row into Pair table if the pair doesn't exist
                            cursor.execute('INSERT INTO "Pair" (token0_id, token1_id) VALUES (%s, %s)', pair)
                            pairs.add(pair)  # Update the set here to avoid duplicate checks
                            pbar.update(1)

        connection.commit()
        print("Pair Table update complete.")
    except (Exception, psycopg2.Error) as error:
        print("Error while updating Pair Table:", error)

# def insert_pair_table(connection):
#     """
#     Inserts unique token pairs into Pair table using batch execution.
#
#     Optimizations:
#     - Uses execute_batch() to run batched INSERT for performance
#     - Calculates unique pairs beforehand to avoid duplicates
#     - Shows insertion progress with tqdm progress bar
#     """
#     cursor = connection.cursor()
#
#     # Fetch token ids from database
#     cursor.execute('SELECT token_id FROM "Token"')
#     token_ids = [row[0] for row in cursor.fetchall()]
#
#     # Generate unique pairs
#     pairs = set()
#     for i in range(len(token_ids)):
#         for j in range(i + 1, len(token_ids)):
#             pair = tuple(sorted([token_ids[i], token_ids[j]]))
#             pairs.add(pair)
#
#     # SQL statement with placeholders
#     sql = 'INSERT INTO "Pair" (token0_id, token1_id) VALUES (%s, %s)'
#
#     # Total rows to insert
#     total_pairs = len(pairs)
#
#     # Insert pairs in batches and show progress bar
#     with tqdm(total=total_pairs) as pbar:
#         execute_batch(cursor, sql, list(pairs))
#         pbar.update(total_pairs)
#
#     connection.commit()
#
#     print("Pair table populated using optimized batch insertion")

# if __name__ == "__main__":
#     connection = create_connection()
#     clear_pair_table(connection)
#     insert_pair_table(connection)
#     connection.close()