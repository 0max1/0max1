"""
Author: Justin Jia, Zhenhao Lu
Last Updated: October 14, 2023
Version: 1.0.3
"""

import json
import psycopg2
from tqdm import tqdm


# update pool
# The slowest part
# Create a table with (pool_address, token1_address, token2_address)
# def update_pool(cursor, connection, pool_data, blockchain_name, tvl_pool_flag):
#     for pool in tqdm(pool_data, total=len(pool_data), desc="Updating Tables", unit="pool"):
#         # The goal of this part is inserting new protocol name into "Protocol" and give it an id automatically
#         protocol_name = pool["Name"]
#         # update protocol out of for-loop and keep a dictionary???? Maybe overloaded
#         # protocol_id = update_protocol(cursor, connection, protocol_name)
#
#         # The goal of this part is inserting new block chain name
#         pool_address = pool["pool_address"]
#         tvl = pool["tvl"]
#         fee = pool["fee"]
#         pool_flag = False
#         # blockchain_id = update_blockchain(cursor, connection, blockchain_name)


# update pool_pair
# Check next week if a pool has two tokens that at least one of them has a num_holder < bound.
# Should this pair be recorded?
# if no, this function need to be changed a lot
# if yes, it's unnecessary to check flag while inserting tokens into Token
# def update_pool_pair(cursor, connection, pool_id, pool):
#     # Why not directly get pairs from Pair???
#     tokens = []
#     # token1-token9
#     for i in range(1, 9):
#         # Use json file to check the line below to make sure whether 9 is necessary
#         token_address = pool.get(f"token{i}")
#         if token_address:
#             token_symbol = pool.get(f"token{i}_symbol")
#             token_decimals = pool.get(f"token{i}_decimals")
#
#             # Logic Error!!!!!
#             # if-else can be removed by using get(key, default_value)
#             # Try with this link: https://www.w3schools.com/python/python_json.asp
#             if token_symbol and token_decimals:
#                 tokens.append({
#                     "token_address": token_address,
#                     "token_symbol": token_symbol,
#                     "token_decimals": token_decimals
#                 })
#             else:
#                 tokens.append({
#                     "token_address": token_address,
#                     "token_symbol": "Unknown",
#                     "token_decimals": 0
#                 })
#
#     pairs = set()
#     # if there are only 2 tokens for each pool. Loop is unnecessary
#     # print(len(tokens))
#     for i in range(len(tokens)):
#         for j in range(i + 1, len(tokens)):
#             cursor.execute('SELECT token_id FROM "Token" WHERE token_address = %s', (tokens[i]["token_address"],))
#             token1_id = cursor.fetchone()
#             token1_symbol = tokens[i].get("token_symbol", None)
#             token1_decimals = tokens[j].get("token_decimals", None)
#             if not token1_id:
#                 cursor.execute(
#                     'INSERT INTO "Token" (token_address, token_symbol, decimal) VALUES (%s, %s, %s) RETURNING token_id',
#                     (tokens[i]["token_address"], token1_symbol, token1_decimals))
#                 token1_id = cursor.fetchone()[0]
#             else:
#                 token1_id = token1_id[0]
#
#             cursor.execute('SELECT token_id FROM "Token" WHERE token_address = %s', (tokens[j]["token_address"],))
#             token2_id = cursor.fetchone()
#             token2_symbol = tokens[j].get("token_symbol", None)
#             token2_decimals = tokens[j].get("token_decimals", None)
#
#             if not token2_id:
#                 cursor.execute(
#                     'INSERT INTO "Token" (token_address, token_symbol, decimal) VALUES (%s, %s, %s) RETURNING token_id',
#                     (tokens[j]["token_address"], token2_symbol, token2_decimals))
#                 token2_id = cursor.fetchone()[0]
#             else:
#                 token2_id = token2_id[0]
#
#             pair = tuple(sorted([token1_id, token2_id]))
#             if pair not in pairs:
#                 cursor.execute('SELECT pair_id FROM "Pair" WHERE token0_id = %s AND token1_id = %s',
#                                (pair[0], pair[1]))
#                 existing_pair = cursor.fetchone()
#
#                 if existing_pair:
#                     pair_id = existing_pair[0]
#                 else:
#                     cursor.execute('INSERT INTO "Pair" (token0_id, token1_id) VALUES (%s, %s)', pair)
#                     connection.commit()
#
#                     cursor.execute('SELECT lastval()')
#                     pair_id = cursor.fetchone()[0]
#
#                 cursor.execute('SELECT pair_id FROM "Pool_Pair" WHERE pool_id = %s', (pool_id,))
#                 pairs.add(pair)
#
#                 existing_pairs = cursor.fetchall()
#                 existing_pair_ids = {pair_id[0] for pair_id in existing_pairs}
#
#                 # If this pair doesn't exist before, pair_id must not in existing_pair_ids
#                 # if it exists, that means you find it by 2 tokens you got from pool data, and now you want to update
#                 # the data of two tokens?????????
#                 if pair_id in existing_pair_ids:
#                     cursor.execute('SELECT token0_id, token1_id FROM "Pair" WHERE pair_id = %s', (pair_id,))
#                     token1_id, token2_id = cursor.fetchone()
#
#                     # if a pool has multiple pairs (from ER Diagram). This will update all pair_id to the same id???????
#                     if (token1_id, token2_id) != pair:
#                         cursor.execute('UPDATE "Pool_Pair" SET pair_id = %s WHERE pool_id = %s',
#                                        (pair_id, pool_id))
#                         connection.commit()
#                 else:
#                     cursor.execute('INSERT INTO "Pool_Pair" (pool_id, pair_id) VALUES (%s, %s)', (pool_id, pair_id))
#                     connection.commit()


def insert_pool_table(connection, pool_data, blockchain_name, tvl_pool_flag):
    """
       Inserts pool data into the "Pool," "Protocol," and "Pool_Token" tables.

       Args:
           :param blockchain_name: (str) The name of the blockchain.
           :param pool_data: (list) List of pool data.
           :param connection: (psycopg2.extensions.connection) The database connection object.
           :param tvl_pool_flag: (int) the minimum value of tvl of a "useful" pool
    """
    pool_pair_list, pool_list, token_set = prepare_pool_data(connection, pool_data)
    with connection.cursor() as cursor:
        args = ','.join(cursor.mogrify("(%s, %s, %s, %s, %s, %s)",
                                       (pool_info[0], pool_info[1], blockchain_name,
                                        pool_info[2], pool_info[3],
                                        pool_info[2] >= float(tvl_pool_flag))).decode('utf-8')
                        for pool_info in pool_list)
        cursor.execute('INSERT INTO "Pool" VALUES ' + args)
        # insert tokens which are not recorded by token json file into Token Table
        refill_command = ','.join(cursor.mogrify("(%s, %s, %s, %s)", (token, '', 18, 0)).decode('utf-8')
                                  for token in token_set)
        cursor.execute('INSERT INTO "Token" VALUES ' + refill_command + ' ON CONFLICT DO NOTHING')
        insert_command = ','.join(cursor.mogrify("(%s, %s)", pool_tokens).decode('utf-8')
                                  for pool_tokens in pool_pair_list)
        cursor.execute('INSERT INTO "Pool_Token" VALUES ' + insert_command)
        connection.commit()
        print("Pool, Protocol, Pool_Pair Tables Init complete.")


def prepare_pool_data(connection, pool_data):
    """
    Parse the data from pool json file
    :param connection: (psycopg2.extensions.connection) The database connection object.
    :param pool_data: raw data read from pool json file
    :return:    tokens: all tokens exist in pools
                pool_list: address, blockchain, protocol, tvl and fee of all pools
                pool_pair_list: a list of tuple (pool A, a token in A)
    """
    protocols = dict()
    tokens = set()
    pool_pair_list = []
    pool_list = []
    print("Total Pools: ", len(pool_data))
    for pool in pool_data:
        # add new protocol
        # protocols.add((pool['factory'], pool["Name"]))
        protocols[pool['factory']] = pool['name']
        # collect new pool's information
        pool_info = [pool["contract"], pool["factory"], pool["tvl"], pool["fee"]]
        # look through all tokens included by this pool (1~9)
        # If in the future the structure become "tokens": [token1, token2, ...],
        #   change the 4 lines below
        i = 0
        while i < 9 and pool.get(f"token{i}"):
            tokens.add(pool.get(f"token{i}"))
            pool_pair_list.append((pool["contract"], pool.get(f"token{i}")))
            i += 1
        # add new pool
        pool_list.append(pool_info)
    # Now, all protocol exist in pools are collected. Then insert them into the Protocol Table
    insert_protocol_table(connection, protocols)
    return pool_pair_list, pool_list, tokens


def insert_protocol_table(connection, protocols):
    """

    :param connection: (psycopg2.extensions.connection) The database connection object.
    :param protocols: a dictionary {factory_address: protocol_name}
    """
    with connection.cursor() as cursor:
        insert_command = ','.join(cursor.mogrify("(%s, %s)",
                                                 (protocol, protocols[protocol])).decode('utf-8')
                                  for protocol in list(protocols.keys()))
        cursor.execute('INSERT INTO "Protocol" VALUES ' + insert_command +
                       ' ON CONFLICT(factory_address) DO UPDATE SET protocol_name = EXCLUDED.protocol_name')
        connection.commit()

# if __name__ == "__main__":
#     connection = create_connection()
#     pool_data_path = ""
#     pool_data = read_pool_data(pool_data_path)
#     blockchain_name = ""
#     insert_pool_table(connection, pool_data, blockchain_name)
#     connection.close()
