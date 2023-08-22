"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import psycopg2

# updating BlockChain Table
def update_blockchain_table(connection):
    try:
        cursor = connection.cursor()

        sample_data = [
            {"blockchain_name": "Polygon"},
            {"blockchain_name": "BNB"},
            {"blockchain_name": "ETH"}
        ]

        for data in sample_data:
            blockchain_name = data["blockchain_name"]
            cursor.execute('SELECT blockchain_id FROM "BlockChain" WHERE blockchain_name = %s', (blockchain_name,))
            existing_blockchain = cursor.fetchone()

            if not existing_blockchain:
                cursor.execute('INSERT INTO "BlockChain" (blockchain_name) VALUES (%s)', (blockchain_name,))
                print(f"New blockchain with name {blockchain_name} inserted.")
                connection.commit()

        cursor.close()
        connection.commit()
        print("BlockChain Table updating complete.")
    except (Exception, psycopg2.Error) as error:
        print("Error while updating BlockChain Table", error)