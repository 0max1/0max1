"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import configparser
from multiprocessing import Pool, Manager
import os
from init_mevmax_db import create_connection
from cal_route_data import main as graph_routes_main, load_data, build_graph, get_cal_route_dir, get_tokens, init_graph ,add_edges , get_params, generate_folder, chunk_pairs, process_chunk, create_json, default_serializer, generate_filename, get_routes
from cal_route_data import generate_csv_files
from cal_route_data import update_pair_flag

def main():
    # Read configuration from the INI file
    config = configparser.ConfigParser()
    base_path = os.path.dirname(os.path.abspath(__file__))
    ini_path = os.path.join(base_path, "..", "config", "mevmax_config.ini")
    config.read(ini_path)

    # Get paths and parameters from the config
    cal_route_dir = get_cal_route_dir()
    user = config.get('DATABASE', 'user')
    password = config.get('DATABASE', 'password')
    host = config.get('DATABASE', 'host')
    database = config.get('DATABASE', 'database')
    pool_file = os.path.join(cal_route_dir, config.get('GET_ROUTE_DATA', 'pool_file'))
    pair_file = os.path.join(cal_route_dir, config.get('GET_ROUTE_DATA', 'pair_file'))
    num_holders = config.getint('GET_ROUTE_DATA', 'num_holders')
    pairs_limit = config.getint('GET_ROUTE_DATA', 'pairs_limit')
    min_tvl = config.getint('GET_ROUTE_DATA', 'min_tvl')
    depth_limit = config.getint('GET_ROUTE_DATA', 'depth_limit')
    num_processes = config.getint('GET_ROUTE_DATA', 'num_processes')

    # Generate CSV files with pool and pair data
    generate_csv_files(pool_file, pair_file, num_holders, pairs_limit, min_tvl)

    # Initialize a multiprocessing manager for sharing data between processes
    manager = Manager()
    no_routes_pairs = manager.list()

    # Load data and build a graph
    pools, pairs = load_data()
    graph = build_graph(pools, pairs)

    # Get blockchain information
    blockchain_name, blockchain_id = get_params(pools)

    # Determine chunk size based on depth limit and data size
    if depth_limit == 1:
        chunk_size = 50000
    elif depth_limit == 2:
        chunk_size = 5000
    elif depth_limit == 3:
        chunk_size = 100
    if len(pairs) < chunk_size:
        chunk_size = len(pairs)

    # Generate a folder for the results
    folder_name = generate_folder(depth_limit, pairs.shape[0], blockchain_name, blockchain_id)
    os.makedirs(folder_name, exist_ok=True)

    # Chunk pairs and process them in parallel
    chunked_pairs = chunk_pairs(pairs, chunk_size)

    with Pool(processes=num_processes) as pool:
        chunk_params = [(pairs_chunk, depth_limit, blockchain_name, blockchain_id, i + 1, folder_name, graph, no_routes_pairs) for
                        i, pairs_chunk in enumerate(chunked_pairs)]
        pool.starmap(process_chunk, chunk_params)

    # Save pairs with no routes to a text file
    no_routes_pairs_txt_name = os.path.join(folder_name, "no_routes_pairs.txt")
    with open(no_routes_pairs_txt_name, "w") as f:
        f.write("\n".join(map(str, no_routes_pairs)))

    # Create a database connection and update pair flags
    connection = create_connection(user, password, host, database)
    update_pair_flag(connection, no_routes_pairs_txt_name)

if __name__ == '__main__':
    main()