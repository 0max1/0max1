"""
Author: Justin Jia, Zhenhao Lu
Last Updated: October 14, 2023
Version: 1.0.3
"""

import json
import psycopg2
from tqdm import tqdm
from init_mevmax_db.init_pool_protocol_poolpair_data import prepare_pool_data


# update protocol
# duplicate code
# def update_protocol(cursor, connection, protocol_name):
#     cursor.execute('SELECT protocol_id FROM "Protocol" WHERE protocol_name = %s', (protocol_name,))
#     protocol_id = cursor.fetchone()
#
#     if not protocol_id:
#         cursor.execute('INSERT INTO "Protocol" (protocol_name) VALUES (%s)', (protocol_name,))
#         print(f"New protocol with name {protocol_name} inserted.")
#         connection.commit()
#
#     cursor.execute('SELECT protocol_id FROM "Protocol" WHERE protocol_name = %s', (protocol_name,))
#     protocol_id = cursor.fetchone()[0]
#     return protocol_id


# update blockchain
# Useless function
# def update_blockchain(cursor, connection, blockchain_name):
#     cursor.execute('SELECT blockchain_id FROM "BlockChain" WHERE blockchain_name = %s', (blockchain_name,))
#     blockchain_id = cursor.fetchone()
#
#     if not blockchain_id:
#         cursor.execute('INSERT INTO "BlockChain" (blockchain_name) VALUES (%s)', (blockchain_name,))
#         print(f"New blockchain with name '{blockchain_name}' inserted.")
#         connection.commit()
#
#         cursor.execute('SELECT lastval()')
#         blockchain_id = cursor.fetchone()[0]
#     else:
#         blockchain_id = blockchain_id[0]
#
#     return blockchain_id


# update pool
# nearly the same as init_pool_protocol_poolpair_data.update_pool()
# def update_pool(cursor, connection, pool_data, blockchain_name, tvl_pool_flag):
#     for pool in tqdm(pool_data, total=len(pool_data), desc="Updating Tables 1", unit="pool"):
#         protocol_name = pool["Name"]
#         protocol_id = update_protocol(cursor, connection, protocol_name)
#
#         pool_address = pool["pool_address"]
#         tvl = pool["tvl"]
#         fee = pool["fee"]
#         pool_flag = False
#         # Unknown use. All pool in the loop should be in the same blockchain_id and this id can be gotten before and
#         # use later as a parameter of this function
#         blockchain_id = update_blockchain(cursor, connection, blockchain_name)
#
#         cursor.execute('SELECT * FROM "Pool" WHERE pool_address = %s', (pool_address,))
#         existing_pool = cursor.fetchone()
#         # The line below and if tvl>=.... can be changed to a single statement. It should be move before the query above
#         tvl_pool_flag = float(tvl_pool_flag)
#         if tvl >= tvl_pool_flag:
#             pool_flag = True
#         # Here maybe we can use Replace or Insert Ignore to combine Update and Insert
#         # See this: https://stackoverflow.com/questions/1361340/how-can-i-do-insert-if-not-exists-in-mysql
#         if existing_pool:
#             pool_id = existing_pool[0]
#             cursor.execute(
#                 'UPDATE "Pool" SET protocol_id = %s, blockchain_id = %s, tvl = %s, fee = %s, pool_flag = %s WHERE pool_id = %s',
#                 (protocol_id, blockchain_id, tvl, fee, pool_flag, pool_id))
#             # print(f"Pool with address {pool_address} updated.")
#         else:
#             cursor.execute(
#                 'INSERT INTO "Pool" (pool_address, protocol_id, blockchain_id, tvl, fee, pool_flag) VALUES (%s, %s, %s, %s, %s, %s)',
#                 (pool_address, protocol_id, blockchain_id, tvl, fee, pool_flag))
#             # print(f"New pool with address {pool_address} inserted.")
#             connection.commit()
#
#             cursor.execute('SELECT lastval()')
#             pool_id = cursor.fetchone()[0]
#
#         update_pool_pair(cursor, connection, pool_id, pool)


# update pool_pair
# def update_pool_pair(cursor, connection, pool_id, pool):
#     tokens = []
#     # tokens = [{"token_address": pool.get(f"token{i}", None),
#     #            "token_symbol": pool.get(f"token{i}_symbol", "Unknown"),
#     #            "token_decimals": pool.get(f"token{i}_decimals")} for i in range(1, 9)
#     #           if pool.get(f"token{i}") and pool.get(f"token{i}") != 'null']
#     for i in range(1, 9):
#         token_address = pool.get(f"token{i}")
#         if token_address and token_address != 'null':
#             token_symbol = pool.get(f"token{i}_symbol")
#             token_decimals = pool.get(f"token{i}_decimals")
#
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
#     # This version is a little better than init_pool_protocol_poolpair_data function.
#     # However, the basic structure is the same
#     for i in range(len(tokens)):
#         for j in range(i + 1, len(tokens)):
#             cursor.execute('SELECT token_id FROM "Token" WHERE token_address = %s', (tokens[i]["token_address"],))
#             token1_id = cursor.fetchone()
#
#             if not token1_id:
#                 cursor.execute(
#                     'INSERT INTO "Token" (token_address, token_symbol, decimal) VALUES (%s, %s, %s) RETURNING token_id',
#                     (tokens[i]["token_address"], tokens[i]["token_symbol"], tokens[i]["token_decimals"]))
#                 token1_id = cursor.fetchone()[0]
#             else:
#                 token1_id = token1_id[0]
#
#             cursor.execute('SELECT token_id FROM "Token" WHERE token_address = %s', (tokens[j]["token_address"],))
#             token2_id = cursor.fetchone()
#
#             if not token2_id:
#                 cursor.execute(
#                     'INSERT INTO "Token" (token_address, token_symbol, decimal) VALUES (%s, %s, %s) RETURNING token_id',
#                     (tokens[j]["token_address"], tokens[j]["token_symbol"], tokens[j]["token_decimals"]))
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
#                 if pair_id in existing_pair_ids:
#                     cursor.execute('SELECT token0_id, token1_id FROM "Pair" WHERE pair_id = %s', (pair_id,))
#                     token1_id, token2_id = cursor.fetchone()
#
#                     if (token1_id, token2_id) != pair:
#                         cursor.execute('UPDATE "Pool_Pair" SET pair_id = %s WHERE pool_id = %s',
#                                        (pair_id, pool_id))
#                         connection.commit()
#                 else:
#                     cursor.execute('INSERT INTO "Pool_Pair" (pool_id, pair_id) VALUES (%s, %s)', (pool_id, pair_id))
#                     connection.commit()


def update_pool_table(connection, pool_data, blockchain_name, tvl_pool_flag):
    """

    :param connection: (psycopg2.extensions.connection) The database connection object.
    :param pool_data: raw data from pool_json file or data in the same json format
    :param blockchain_name: The same as the database name
    :param tvl_pool_flag: min_tvl
    """
    # see prepare_pool_data in init_pool_protocol_poolpair_data
    pool_pair_list, pool_list, token_set = prepare_pool_data(connection, pool_data)
    with connection.cursor() as cursor:
        # Insert or Update pools in Pool Table
        args = ','.join(cursor.mogrify("(%s, %s, %s, %s, %s, %s)",
                                       (pool_info[0], pool_info[1], blockchain_name,
                                        pool_info[2], pool_info[3],
                                        pool_info[2] >= float(tvl_pool_flag))).decode('utf-8')
                        for pool_info in pool_list)
        cursor.execute('INSERT INTO "Pool" VALUES ' +
                       args +
                       ' ON CONFLICT (pool_address) DO UPDATE SET '
                       'blockchain_name = EXCLUDED.blockchain_name, protocol_name = EXCLUDED.protocol_name, '
                       'tvl = EXCLUDED.tvl, fee = EXCLUDED.fee, pool_flag = EXCLUDED.pool_flag')
        # Create a temp table to record all (maybe) changed pools in this update
        cursor.execute('DROP TABLE IF EXISTS "Temp"')
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "Temp"(
                "pool_address" VARCHAR(50),
                "token_address" VARCHAR(50),
                PRIMARY KEY (pool_address, token_address))
        """)
        # insert all (maybe) changed pools
        insert_command = ','.join(cursor.mogrify("(%s, %s)", pool_tokens).decode('utf-8')
                                  for pool_tokens in pool_pair_list)
        cursor.execute('INSERT INTO "Temp" VALUES ' + insert_command)
        # Delete all records related to those pools because some tokens may not exist in some pools any more and these
        #   pool_token pairs need to be deleted
        clear_command = f"""
            DELETE FROM "Pool_Token" WHERE pool_address IN (SELECT DISTINCT pool_address FROM "Temp")
        """
        cursor.execute(clear_command)
        # insert tokens which are not recorded by token json file or Token Table into Token Table
        refill_command = f"""
                    INSERT INTO "Token"
                    SELECT DISTINCT Te.token_address, '', 18, 0
                    FROM "Temp" as Te
                    ON CONFLICT DO NOTHING
                """
        cursor.execute(refill_command)
        # insert pool_token pairs
        pool_pair_command = f"""INSERT INTO "Pool_Token" SELECT pool_address, token_address FROM "Temp" """
        cursor.execute(pool_pair_command)
        # Delete this temp table
        cursor.execute('DROP TABLE IF EXISTS "Temp"')
        connection.commit()
        print("Pool, Protocol, Pool_Pair Tables update complete.")

# if __name__ == "__main__":
#     connection = create_connection()
#     pool_data_path = ""
#     pool_data = read_pool_data(pool_data_path)
#     blockchain_name = ""
#     update_pool_table(connection, pool_data, blockchain_name)
#     connection.close()
