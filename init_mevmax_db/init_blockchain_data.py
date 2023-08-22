"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import psycopg2

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

def initialize_blockchain_table(connection):
    """
    Initializes the "BlockChain" table with sample data if it doesn't exist.

    Args:
        connection (psycopg2.extensions.connection): The database connection object.
    """
    try:
        cursor = connection.cursor()

        sample_data = [
            {"blockchain_name": "Polygon"},
            {"blockchain_name": "BNB"},
            {"blockchain_name": "ETH"}
        ]

        for data in sample_data:
            blockchain_name = data["blockchain_name"]
            # Check if the blockchain already exists in the table
            cursor.execute('SELECT blockchain_id FROM "BlockChain" WHERE blockchain_name = %s', (blockchain_name,))
            existing_blockchain = cursor.fetchone()

            if not existing_blockchain:
                # Insert a new blockchain if it doesn't exist
                cursor.execute('INSERT INTO "BlockChain" (blockchain_name) VALUES (%s)', (blockchain_name,))
                print(f"New blockchain with name {blockchain_name} inserted.")
                connection.commit()

        cursor.close()
        connection.commit()
        print("BlockChain Table initialization complete.")
    except (Exception, psycopg2.Error) as error:
        print("Error while initializing BlockChain Table:", error)

# if __name__ == "__main__":
#     connection = create_connection()
#     if connection:
#         initialize_blockchain_table(connection)
#         connection.close()