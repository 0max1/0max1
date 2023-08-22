"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import json
import psycopg2
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

def read_pool_data(pool_data_path):
    """
    Reads pool data from a JSON file.

    Args:
        pool_data_path (str): Path to the JSON file containing pool data.

    Returns:
        list: List of pool data.
    """
    with open(pool_data_path, "r") as f:
        data = json.load(f)
    return data


# update protocol
def update_protocol(cursor, connection, protocol_name):
    """
        Updates the "Protocol" table or inserts a new row if the protocol doesn't exist.

        Args:
            cursor (psycopg2.extensions.cursor): The database cursor object.
            connection (psycopg2.extensions.connection): The database connection object.
            protocol_name (str): The name of the protocol.

        Returns:
            int: The protocol ID.
        """
    cursor.execute('SELECT protocol_id FROM "Protocol" WHERE protocol_name = %s', (protocol_name,))
    protocol_id = cursor.fetchone()

    if not protocol_id:
        cursor.execute('INSERT INTO "Protocol" (protocol_name) VALUES (%s)', (protocol_name,))
        print(f"New protocol with name {protocol_name} inserted.")
        connection.commit()

    cursor.execute('SELECT protocol_id FROM "Protocol" WHERE protocol_name = %s', (protocol_name,))
    protocol_id = cursor.fetchone()[0]
    return protocol_id

# update blockchain
def update_blockchain(cursor, connection, blockchain_name):
    cursor.execute('SELECT blockchain_id FROM "BlockChain" WHERE blockchain_name = %s', (blockchain_name,))
    blockchain_id = cursor.fetchone()

    if not blockchain_id:
        cursor.execute('INSERT INTO "BlockChain" (blockchain_name) VALUES (%s)', (blockchain_name,))
        print(f"New blockchain with name '{blockchain_name}' inserted.")
        connection.commit()

        cursor.execute('SELECT lastval()')
        blockchain_id = cursor.fetchone()[0]
    else:
        blockchain_id = blockchain_id[0]

    return blockchain_id

# update pool
def update_pool(cursor, connection, pool_data, blockchain_name):
    for pool in tqdm(pool_data, total=len(pool_data), desc="Updating Tables", unit="pool"):
        protocol_name = pool["Name"]
        protocol_id = update_protocol(cursor, connection, protocol_name)

        pool_address = pool["pool_address"]
        tvl = pool["tvl"]
        fee = pool["fee"]

        blockchain_id = update_blockchain(cursor, connection, blockchain_name)

        cursor.execute('SELECT * FROM "Pool" WHERE pool_address = %s', (pool_address,))
        existing_pool = cursor.fetchone()

        if existing_pool:
            pool_id = existing_pool[0]
            cursor.execute(
                'UPDATE "Pool" SET protocol_id = %s, blockchain_id = %s, tvl = %s, fee = %s WHERE pool_id = %s',
                (protocol_id, blockchain_id, tvl, fee, pool_id))
            # print(f"Pool with address {pool_address} updated.")
        else:
            cursor.execute(
                'INSERT INTO "Pool" (pool_address, protocol_id, blockchain_id, tvl, fee) VALUES (%s, %s, %s, %s, %s)',
                (pool_address, protocol_id, blockchain_id, tvl, fee))
            # print(f"New pool with address {pool_address} inserted.")
            connection.commit()

            cursor.execute('SELECT lastval()')
            pool_id = cursor.fetchone()[0]

        update_pool_pair(cursor, connection, pool_id, pool)

# update pool_pair
def update_pool_pair(cursor, connection, pool_id, pool):
    tokens = []
    for i in range(1, 9):
        token_address = pool.get(f"token{i}")
        if token_address:
            token_symbol = pool.get(f"token{i}_symbol")
            token_decimals = pool.get(f"token{i}_decimals")
            tokens.append({
                "token_address": token_address,
                "token_symbol": token_symbol,
                "token_decimals": token_decimals
            })

    pairs = set()

    for i in range(len(tokens)):
        for j in range(i + 1, len(tokens)):
            cursor.execute('SELECT token_id FROM "Token" WHERE token_address = %s', (tokens[i]["token_address"],))
            token1_id = cursor.fetchone()
            token1_symbol = tokens[i].get("token_symbol", None)
            token1_decimals = tokens[j].get("token_decimals", None)
            if not token1_id:
                cursor.execute(
                    'INSERT INTO "Token" (token_address, token_symbol, decimal) VALUES (%s, %s, %s) RETURNING token_id',
                    (tokens[i]["token_address"], token1_symbol, token1_decimals))
                token1_id = cursor.fetchone()[0]
            else:
                token1_id = token1_id[0]

            cursor.execute('SELECT token_id FROM "Token" WHERE token_address = %s', (tokens[j]["token_address"],))
            token2_id = cursor.fetchone()
            token2_symbol = tokens[j].get("token_symbol", None)
            token2_decimals = tokens[j].get("token_decimals", None)

            if not token2_id:
                cursor.execute(
                    'INSERT INTO "Token" (token_address, token_symbol, decimal) VALUES (%s, %s, %s) RETURNING token_id',
                    (tokens[j]["token_address"], token2_symbol, token2_decimals))
                token2_id = cursor.fetchone()[0]
            else:
                token2_id = token2_id[0]

            pair = tuple(sorted([token1_id, token2_id]))
            if pair not in pairs:
                cursor.execute('SELECT pair_id FROM "Pair" WHERE token0_id = %s AND token1_id = %s',
                               (pair[0], pair[1]))
                existing_pair = cursor.fetchone()

                if existing_pair:
                    pair_id = existing_pair[0]
                else:
                    cursor.execute('INSERT INTO "Pair" (token0_id, token1_id) VALUES (%s, %s)', pair)
                    connection.commit()

                    cursor.execute('SELECT lastval()')
                    pair_id = cursor.fetchone()[0]

                cursor.execute('SELECT pair_id FROM "Pool_Pair" WHERE pool_id = %s', (pool_id,))
                pairs.add(pair)

                existing_pairs = cursor.fetchall()
                existing_pair_ids = {pair_id[0] for pair_id in existing_pairs}

                if pair_id in existing_pair_ids:
                    cursor.execute('SELECT token0_id, token1_id FROM "Pair" WHERE pair_id = %s', (pair_id,))
                    token1_id, token2_id = cursor.fetchone()

                    if (token1_id, token2_id) != pair:
                        cursor.execute('UPDATE "Pool_Pair" SET pair_id = %s WHERE pool_id = %s',
                                       (pair_id, pool_id))
                        connection.commit()
                else:
                    cursor.execute('INSERT INTO "Pool_Pair" (pool_id, pair_id) VALUES (%s, %s)', (pool_id, pair_id))
                    connection.commit()


def insert_pool_table(connection, pool_data, blockchain_name):
    """
       Inserts pool data into the "Pool," "Protocol," and "Pool_Pair" tables.

       Args:
           connection (psycopg2.extensions.connection): The database connection object.
           pool_data (list): List of pool data.
           blockchain_name (str): The name of the blockchain.
    """
    try:
        cursor = connection.cursor()
        with tqdm(total=len(pool_data), desc="Init Tables", unit="pool") as pbar:
            update_pool(cursor, connection, pool_data, blockchain_name)
            cursor.close()
            connection.commit()
            print("Pool, Protocol, Pool_Pair Tables Init complete.")
    except (Exception, psycopg2.Error) as error:
        print("Error while updating Pool, Protocol, Pool_Pair Tables", error)


# if __name__ == "__main__":
#     connection = create_connection()
#     pool_data_path = ""
#     pool_data = read_pool_data(pool_data_path)
#     blockchain_name = ""
#     insert_pool_table(connection, pool_data, blockchain_name)
#     connection.close()