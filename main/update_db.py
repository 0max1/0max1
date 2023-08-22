"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import os

from init_mevmax_db.create_mevmax_db import create_connection
from update_mevmax_db.update_token_pair_data import read_token_data, update_token_table, update_pair_table
from update_mevmax_db.update_blockchain_data import update_blockchain_table
from update_mevmax_db.update_pool_protocol_poolpair_data import read_pool_data, update_pool_table
from generate_original_data.bsc_data_collection_v2 import get_cal_route_dir
import configparser

def main():
    # Load configuration from mevmax_config.ini
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

    # Get the path to the directory containing calculated route data
    cal_route_dir = get_cal_route_dir()

    # Define paths to token and pool data files from the configuration for update
    token_data_path = os.path.join(cal_route_dir, config.get('UPDATE_DB', 'token_data_path'))
    pool_data_path = os.path.join(cal_route_dir, config.get('UPDATE_DB', 'pool_data_path'))

    # Extract blockchain name from the configuration for update
    blockchain_name = config.get('UPDATE_DB', 'blockchain_name')

    if connection:

        # Update the token data
        token_data = read_token_data(token_data_path)
        new_token_addresses = update_token_table(connection, token_data)
        # Update the pair data (insert all combinations between existence tokens)
        update_pair_table(connection, new_token_addresses)
        # Update the blockchain data
        update_blockchain_table(connection)
        # Update the pool, protocol, and pool_pair data. Update the blockchain data
        pool_data = read_pool_data(pool_data_path)
        update_pool_table(connection, pool_data, blockchain_name)

if __name__ == '__main__':
    main()