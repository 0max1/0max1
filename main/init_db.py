"""
Author: Justin Jia， Zhenhao Lu
Last Updated: October 14th, 2023
Version: 1.0.3
"""

import os

from init_mevmax_db.create_mevmax_db import create_connection, drop_tables, create_tables, ConfigReader
from init_mevmax_db.init_token_data import read_json_data, insert_token_table
from init_mevmax_db.init_blockchain_data import initialize_blockchain_table
from init_mevmax_db.init_pair_data import insert_pair_table
from init_mevmax_db.init_pool_protocol_poolpair_data import insert_pool_table
from generate_original_data.bsc_data_collection_v2 import get_cal_route_dir


# original Polygon: 1.5 hour
# new Polygon: 15 seconds
def main():
    ini_reader = ConfigReader()
    user, password, host, database, port = ini_reader.getDatabaseInfo()

    # Create a connection to the PostgreSQL database
    connection = create_connection(user, password, host, database, port=port)

    # Get the path to directory containing calculated route data
    cal_route_dir = get_cal_route_dir()

    token_data_path, pool_data_path, blockchain_name, tvl_pool_flag, holders_pair_flag = ini_reader.getInitInfo()
    token_data_path = os.path.join(cal_route_dir, token_data_path)
    pool_data_path = os.path.join(cal_route_dir, pool_data_path)

    if connection:
        # Drop existing tables and constraints, init token, pair, pool, protocol, blockchain, and pool_pair tables
        drop_tables(connection)
        create_tables(connection)

        # Init the token data
        token_data = read_json_data(token_data_path)
        insert_token_table(connection, token_data)

        # Init the blockchain data
        initialize_blockchain_table(connection)

        # Init the pair data (insert all combinations between existence tokens)
        insert_pair_table(connection, holders_pair_flag)

        # Init the pool, protocol, and pool_pair data.
        pool_data = read_json_data(pool_data_path)
        insert_pool_table(connection, pool_data, blockchain_name, tvl_pool_flag)
    else:
        print("Connection Error")


def initialize_db(token_data, pool_data, database_name=''):
    ini_reader = ConfigReader()
    user, password, host, database, port = ini_reader.getDatabaseInfo()
    if database_name in ['ETH', 'BSC', 'Polygon']:
        database = database_name
    connection = create_connection(user, password, host, database, port=port)
    token_data_path, pool_data_path, blockchain_name, tvl_pool_flag, holders_pair_flag = ini_reader.getInitInfo()
    if connection:
        drop_tables(connection)
        create_tables(connection)
        initialize_blockchain_table(connection)
        insert_token_table(connection, token_data)
        insert_pair_table(connection, holders_pair_flag)
        insert_pool_table(connection, pool_data, blockchain_name, tvl_pool_flag)


if __name__ == '__main__':
    main()
