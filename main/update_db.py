"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import os
from datetime import datetime

from init_mevmax_db.create_mevmax_db import create_connection, ConfigReader
from init_mevmax_db.init_token_data import read_json_data
from update_mevmax_db.update_token_pair_data import update_token_table, update_pair_table
from update_mevmax_db.update_blockchain_data import update_blockchain_table
from update_mevmax_db.update_pool_protocol_poolpair_data import update_pool_table
# from generate_original_data.bsc_data_collection_v2 import get_cal_route_dir
import configparser


# depth = 3: 240+hours
# depth = 2: 6+ hours
def main():
    # Load configuration from mevmax_config.ini
    config = ConfigReader()
    user, password, host, database, port = config.getDatabaseInfo()

    # Create a connection to the PostgreSQL database
    connection = create_connection(user, password, host, database, port=port)

    # Get the path to the directory containing calculated route data
    # cal_route_dir = get_cal_route_dir()
    cal_route_dir = ''

    # Define paths to token and pool data files from the configuration for update
    token_data_path, pool_data_path, blockchain_name, tvl_pool_flag, holders_pair_flag = config.getUpdateInfo()
    token_data_path = os.path.join(cal_route_dir, token_data_path)
    pool_data_path = os.path.join(cal_route_dir, pool_data_path)

    if connection:
        # Update the token data
        token_data = read_json_data(token_data_path)
        update_token_table(connection, token_data)
        # Update the pair data (insert all combinations between existence tokens)
        update_pair_table(connection, holders_pair_flag)
        # Update pool data
        pool_data = read_json_data(pool_data_path)
        update_pool_table(connection, pool_data, blockchain_name, tvl_pool_flag)


def update_db(token_data, pool_data, database_name=''):
    config = ConfigReader()
    user, password, host, database, port = config.getDatabaseInfo()
    if database_name in ['ETH', 'BSC', 'Polygon']:
        database = database_name
    connection = create_connection(user, password, host, database, port=port)
    token_data_path, pool_data_path, blockchain_name, tvl_pool_flag, holders_pair_flag = config.getUpdateInfo()
    print("Ready to connect")
    if connection:
        print("Connection Complete")
        update_token_table(connection, token_data)
        update_pair_table(connection, holders_pair_flag)
        update_pool_table(connection, pool_data, blockchain_name, tvl_pool_flag)


if __name__ == '__main__':
    main()
