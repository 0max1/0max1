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
from generate_original_data.bsc_data_collection_v2 import get_cal_route_dir
import configparser


def main():
    start = datetime.now()
    # Load configuration from mevmax_config.ini
    config = ConfigReader()
    user, password, host, database, port = config.getDatabaseInfo()
    # config = configparser.ConfigParser()
    # base_path = os.path.dirname(os.path.abspath(__file__))
    # ini_path = os.path.join(base_path, "..", "config", "mevmax_config.ini")
    # config.read(ini_path)
    #
    # # Extract database connection parameters from the configuration
    # user = config.get('DATABASE', 'user')
    # password = config.get('DATABASE', 'password')
    # host = config.get('DATABASE', 'host')
    # database = config.get('DATABASE', 'database')
    # port = config.get('DATABASE', 'port')

    # Create a connection to the PostgreSQL database
    connection = create_connection(user, password, host, database, port=port)

    # Get the path to the directory containing calculated route data
    cal_route_dir = get_cal_route_dir()

    # Define paths to token and pool data files from the configuration for update
    token_data_path, pool_data_path, blockchain_name, tvl_pool_flag, holders_pair_flag = config.getUpdateInfo()
    token_data_path = os.path.join(cal_route_dir, token_data_path)
    pool_data_path = os.path.join(cal_route_dir, pool_data_path)

    # Extract blockchain name from the configuration for update
    # blockchain_name = config.get('UPDATE_DB', 'blockchain_name')
    # tvl_pool_flag = config.get('UPDATE_DB', 'tvl_pool_flag')
    # holders_pair_flag = config.get('UPDATE_DB', 'holders_pair_flag')

    if connection:
        # Update the token data
        token_data = read_json_data(token_data_path)
        # token_data = read_token_data(token_data_path)
        update_token_table(connection, token_data)
        # Update the pair data (insert all combinations between existence tokens)
        update_pair_table(connection, holders_pair_flag)
        # Update the blockchain data
        # update_blockchain_table(connection)
        # Update the pool, protocol, and pool_pair data. Update the blockchain data
        pool_data = read_json_data(pool_data_path)
        # pool_data = read_pool_data(pool_data_path)
        update_pool_table(connection, pool_data, blockchain_name, tvl_pool_flag)
        print(datetime.now() - start)
        print("Test End")


if __name__ == '__main__':
    main()
