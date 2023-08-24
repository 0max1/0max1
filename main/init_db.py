"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import configparser
import os

from init_mevmax_db.create_mevmax_db import create_connection, drop_tables, create_tables
from init_mevmax_db.init_token_data import read_token_data, insert_token_table
from init_mevmax_db.init_blockchain_data import initialize_blockchain_table
from init_mevmax_db.init_pair_data import insert_pair_table
from init_mevmax_db.init_pool_protocol_poolpair_data import read_pool_data, insert_pool_table
from generate_original_data.bsc_data_collection_v2 import get_cal_route_dir

def main():
    # Load configuration from the INI file
    config = configparser.ConfigParser()
    base_path = os.path.dirname(os.path.abspath(__file__))
    ini_path = os.path.join(base_path, "..", "config", "mevmax_config.ini")
    config.read(ini_path)

    # Extract database connection parameters from the configuration
    user = config.get('DATABASE', 'user')
    password = config.get('DATABASE', 'password')
    host = config.get('DATABASE', 'host')
    database = config.get('DATABASE', 'database')
    # Create a connection to the PostgreSQL database
    connection = create_connection(user, password, host, database)

    # Get the path to directory containing calculated route data
    cal_route_dir = get_cal_route_dir()

    # Define paths to token and pool data files from the configuration
    token_data_path = os.path.join(cal_route_dir, config.get('INIT_DB', 'token_data_path'))
    pool_data_path = os.path.join(cal_route_dir, config.get('INIT_DB', 'pool_data_path'))

    # Extract blockchain name from the configuration
    blockchain_name = config.get('INIT_DB', 'blockchain_name')
    tvl_pool_flag = config.get('INIT_DB', 'tvl_pool_flag')
    holders_pair_flag = config.get('INIT_DB', 'holders_pair_flag')


    if connection:
        # Drop existing tables and constraints, init token, pair, pool, protocol, blockchain, and pool_pair tables
        drop_tables(connection)
        create_tables(connection)

        # Init the token data

        token_data = read_token_data(token_data_path)
        insert_token_table(connection, token_data)

        # Init the blockchain data
        initialize_blockchain_table(connection)

        # Init the pair data (insert all combinations between existence tokens)
        insert_pair_table(connection, holders_pair_flag)

        # Init the pool, protocol, and pool_pair data. Update the blockchain data
        pool_data = read_pool_data(pool_data_path)
        insert_pool_table(connection, pool_data, blockchain_name, tvl_pool_flag)

if __name__ == '__main__':
    main()

