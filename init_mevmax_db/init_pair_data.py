"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""
from web3 import Web3
# import psycopg2


# def clear_pair_table(connection):
#     """
#     Clears the "Pair" table and related rows in the "Pool_Pair" table.
#
#     Args:
#         connection (psycopg2.extensions.connection): The database connection object.
#     """
#     # Is the situation possible: pair_id in Pool_Pair but not in Pair??????
#     try:
#         cursor = connection.cursor()
#         # cursor.execute('DELETE FROM "Pool_Pair"')
#         cursor.execute('DELETE FROM "Pool_Pair" WHERE pair_id IN (SELECT pair_id FROM "Pair")')
#         cursor.execute('DELETE FROM "Pair"')
#         connection.commit()
#         print("Pair Table and related Pool_Pair rows cleared.")
#         cursor.close()
#     except (Exception, psycopg2.Error) as error:
#         print("Error while clearing Pair Table and related Pool_Pair rows", error)


def insert_pair_table(connection, holders_pair_flag):
    """
    Inserts pairs of tokens into the "Pair" table.

    Args:
        :param connection: (psycopg2.extensions.connection) The database connection object.
        :param holders_pair_flag: the minimum number of holder of a "useful" token
    """
    with connection.cursor() as cursor:
        serialize = Web3()
        # Estimate the number of pairs
        cursor.execute(f'SELECT COUNT(*) FROM "Token" WHERE num_holders >= {holders_pair_flag}')
        pair_size = cursor.fetchone()[0]
        pair_size = pair_size * (pair_size - 1) / 2
        offset = 0
        print("Total Pairs: ", pair_size)
        # Process 2000000 pairs in an iteration. "ORDER BY" is necessary
        # Extract all possible pairs from Token Table
        while offset < min(pair_size, 5000000):
            cursor.execute(f"""
                SELECT T1.token_address, T2.token_address
                FROM "Token" T1
                JOIN "Token" T2 ON T1.token_address < T2.token_address
                WHERE T1.num_holders >= {holders_pair_flag} AND T2.num_holders >= {holders_pair_flag}
                ORDER BY T1.token_address ASC, T2.token_address ASC
                LIMIT 1000000 OFFSET {offset}
            """)
            pairs = cursor.fetchall()
            # Compute their pair_address with keccak-256
            if len(pairs) > 0:
                print("Data collected")
                insert_content = ','.join(cursor.mogrify("(%s, %s, %s)",
                                                         (serialize.keccak(text=pair[0] + pair[1]).hex(),
                                                          pair[0], pair[1])).decode('utf-8') for pair in pairs)
                print("Command Build")
                cursor.execute(
                    'INSERT INTO "Pair" (pair_address, token1_address, token2_address) VALUES ' + insert_content)
            else:
                break
            offset += 1000000
            print(f"~{offset} pairs have been inserted")
        cursor.execute(f'UPDATE "Token" SET is_new = FALSE WHERE num_holders >= {holders_pair_flag}')
        connection.commit()
        print("Pair Table initialization complete.")

    # with connection.cursor() as cursor:
    #     get_all_pairs = f"""
    #         SELECT T1.token_address, T2.token_address
    #         FROM "Token" T1 INNER JOIN "Token" T2 ON T1.token_address < T2.token_address
    #         WHERE T1.num_holders >= {holders_pair_flag} AND T2.num_holders >= {holders_pair_flag}
    #     """
    #     cursor.execute(get_all_pairs)
    #     pairs = cursor.fetchall()
    #     serialize = Web3()
    #     # Danger: 1 min 47 s for 897130 pairs
    #     # This while loop is for preventing out of memory because this program need to process a lot of pairs
    #     i = 0
    #     while i < len(pairs):
    #         insert_content = ','.join(
    #             cursor.mogrify("(%s, %s, %s)",
    #                            (serialize.keccak(text=pair[0] + pair[1]).hex(),
    #                             pair[0], pair[1])).decode('utf-8') for pair in pairs[i: i + 1000000])
    #         cursor.execute('INSERT INTO "Pair" (pair_address, token1_address, token2_address) VALUES ' + insert_content)
    #         i += 1000000
    #         print("Pair Processed: ", i)
    #     cursor.execute(f'UPDATE "Token" SET is_new = FALSE WHERE num_holders >= {holders_pair_flag}')
    #     connection.commit()
    #     print("Pair Table initialization complete.")

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
