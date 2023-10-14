"""
Author: Justin Jia, Zhenhao Lu
Last Updated: October 14, 2023
Version: 1.0.2
"""

import psycopg2


def initialize_blockchain_table(connection):
    """
    Initializes the "BlockChain" table with sample data if it doesn't exist.

    Args:
        connection (psycopg2.extensions.connection): The database connection object.
    """
    with connection.cursor() as cursor:
        args = ','.join(cursor.mogrify("(%s)", (block_chain,)).decode('utf-8')
                        for block_chain in ['Polygon', 'BSC', 'ETH'])
        cursor.execute('INSERT INTO "BlockChain" (blockchain_name) VALUES ' + args)
        connection.commit()
        print("BlockChain Table initialization complete.")

# if __name__ == "__main__":
#     connection = create_connection()
#     if connection:
#         initialize_blockchain_table(connection)
#         connection.close()
