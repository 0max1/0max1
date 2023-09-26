"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import configparser
import json
import os
from datetime import datetime

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
    # # Load configuration from the INI file
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

    # Get the path to directory containing calculated route data
    cal_route_dir = get_cal_route_dir()

    token_data_path, pool_data_path, blockchain_name, tvl_pool_flag, holders_pair_flag = ini_reader.getInitInfo()
    token_data_path = os.path.join(cal_route_dir, token_data_path)
    pool_data_path = os.path.join(cal_route_dir, pool_data_path)

    # Define paths to token and pool data files from the configuration
    # token_data_path = os.path.join(cal_route_dir, config.get('INIT_DB', 'token_data_path'))
    # pool_data_path = os.path.join(cal_route_dir, config.get('INIT_DB', 'pool_data_path'))

    # Extract blockchain name from the configuration
    # blockchain_name = config.get('INIT_DB', 'blockchain_name')
    # tvl_pool_flag = config.get('INIT_DB', 'tvl_pool_flag')
    # holders_pair_flag = config.get('INIT_DB', 'holders_pair_flag')

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

        # Init the pool, protocol, and pool_pair data. Update the blockchain data
        pool_data = read_json_data(pool_data_path)
        # print(len(pool_data))
        # record = dict()
        # duplicate_count = dict()
        # for pool in pool_data:
        #     if pool['pool_address'] not in record:
        #         record[pool['pool_address']] = [pool]
        #         duplicate_count[pool['pool_address']] = 1
        #     else:
        #         duplicate_count[pool['pool_address']] += 1
        #         all_new = True
        #         for old in record[pool['pool_address']]:
        #             temp_new = False
        #             for key in pool:
        #                 if key not in old or pool[key] != old[key]:
        #                     temp_new = True
        #                     break
        #             all_new = all_new and temp_new
        #             if not all_new:
        #                 break
        #         if all_new:
        #             record[pool['pool_address']].append(pool)
        # final_result = []
        # for pool_address in list(duplicate_count.keys()):
        #     if duplicate_count[pool_address] > 1:
        #         duplicate_sit = {'pool_address': pool_address, 'duplicate_case_num': duplicate_count[pool_address],
        #                          'different_cases_num': len(record[pool_address]),
        #                          'different_cases': record[pool_address]}
        #         final_result.append(duplicate_sit)
        # print(len(final_result))
        # with open('check_result.json', 'w+') as f:
        #     json.dump(final_result, f, indent=4)
        insert_pool_table(connection, pool_data, blockchain_name, tvl_pool_flag)


if __name__ == '__main__':
    main()
