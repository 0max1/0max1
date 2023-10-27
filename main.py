"""
MevMax Project

Author: Zhenhao Lu
Last Updated: October 27, 2023
Version: 1.0.4

Description:
This script performs data processing and analysis for the MevMax project,
facilitating efficient data retrieval, transformation, and storage.
"""

import configparser
import os

from main import init_db_main, update_db_main, get_route_data_main, get_row_data_main
import sys


def main():
    # Read the configuration from the ini file
    config = configparser.ConfigParser()
    base_path = os.path.dirname(os.path.abspath(__file__))
    ini_path = os.path.join(base_path, ".", "config", "mevmax_config.ini")
    config.read(ini_path)

    # Get the operation type from the configuration
    op_type = config.get('MAIN', 'op_type')

    # Based on the operation type, perform corresponding actions
    if op_type == "update":
        # get_row_data_main()  # Execute functions for updating data
        update_db_main()
        # get_route_data_main()
    elif op_type == "init":
        # get_row_data_main()  # Execute functions for initializing data
        init_db_main()
        # get_route_data_main()
    else:
        print("Invalid operation type provided. Please enter 'update' or 'init'.")


if __name__ == '__main__':
    main()
