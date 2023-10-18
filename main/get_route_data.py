"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

from multiprocessing import Pool, Manager
import pandas as pd
import numpy as np
import os
from init_mevmax_db import create_connection
# from cal_route_data import main as graph_routes_main, load_data, build_graph, get_cal_route_dir, get_tokens,
# init_graph, \
#     add_edges, get_params, generate_folder, chunk_pairs, process_chunk, create_json, default_serializer, \
#     generate_filename, get_routes, create_result
from cal_route_data.generate_pairs_pools_data import get_pairs, get_pool_pairs, get_blockchain_name
from cal_route_data.cal_route_pairs_data_multithreading_fix_bugs_v2_1 import build_graph, get_pair_routes, \
    generate_folder, chunk_pairs, create_result, build_temp
from init_mevmax_db.create_mevmax_db import ConfigReader


def main():
    # Read configuration from the INI file
    config = ConfigReader()
    user, password, host, database, port = config.getDatabaseInfo()
    connection = create_connection(user, password, host, database, port=port)
    num_holders, pairs_limit, min_tvl, depth_limit, num_processes, max_route, chunk_size = config.getRouteInfo()

    # Get raw pool data and amount of pairs from database
    pools, pair_size = get_pool_pairs(connection, min_tvl, num_holders)
    # Build a temp table for updating
    build_temp(connection)
    # Build graph
    graph = build_graph(pools)
    # Get the blockchain name (name of connected database) from database
    blockchain_name = get_blockchain_name(connection)

    # Initialize a multiprocessing manager for sharing data between processes
    manager = Manager()
    route_pairs = manager.list()

    # Generate a folder for the results
    folder_name, folder_name_s = generate_folder(depth_limit, pair_size, blockchain_name)
    os.makedirs(folder_name, exist_ok=True)

    # compute routes for pairs in groups to prevent out of memory
    # Each group will generate at least 1 result file unless no pair in a group has routes
    offset = 0
    file_index = 0
    old_file_index = 0
    while offset < pair_size:
        print(f"Processing {offset} to {offset + pairs_limit}")
        # Get a group of pairs
        pairs = get_pairs(connection, num_holders, pairs_limit, offset)
        print("Pair Search Complete")
        # Divide this group of pairs again for multithread processing
        pairs = np.array(pairs)
        chunk_size = min(chunk_size, pair_size)
        chunked_pairs = chunk_pairs(pairs, chunk_size)
        with Pool(processes=num_processes) as pool:
            chunk_params = [(pairs_chunk, graph, depth_limit, route_pairs, i)
                            for i, pairs_chunk in enumerate(chunked_pairs)]
            # compute routes
            pool.starmap(get_pair_routes, chunk_params)
        # Processing Routes
        routes_pairs = list(route_pairs)
        route_pairs[:] = []
        current_route_num = 0
        start = 0
        has_routes = []  # Use to record will pair should be updated in database
        with connection.cursor() as cursor:
            # To keep each result file's size at about 10 MB, each file should contain about 7000 routes
            for i in range(len(routes_pairs)):
                has_routes.append(routes_pairs[i]['id'])
                current_route_num += routes_pairs[i]['routes_num']
                # check if there are enough routes to generate result file or if there is no result left
                if current_route_num >= max_route or i == len(routes_pairs) - 1:
                    # create the head of result file
                    routes_results = {
                        "blockchain_name": blockchain_name,
                        "max_depth": depth_limit,
                        "min_tvl": min_tvl,
                        "min_holder": num_holders,
                        "total_pairs": len(pairs),
                        "total_pairs_find_routes": len(routes_pairs),
                        "total_pairs_with_null_routes": len(pairs) - len(routes_pairs),
                        "generated_on": pd.Timestamp.now(),
                        "token_pairs": routes_pairs[start: i + 1]
                    }
                    # save those result into files and save those pairs into a temp table in database
                    create_result(routes_results, folder_name, file_index, has_routes, cursor, folder_name_s)
                    has_routes.clear()
                    file_index += 1  # Will not effect the result. This variable is only for printing logs below
                    current_route_num = 0  # counting for next 7000 (or other value set in ini file)
                    start = i + 1  # point to the result which will be put first into the next result file
            connection.commit()
        offset += pairs_limit
        print(f'Results Generated in {file_index - old_file_index} Files')
        old_file_index = file_index
    print("Updating Results to Database")
    with connection.cursor() as cursor:
        # while insert_offset < pair_size:
        #     cursor.execute(f"""
        #         INSERT INTO "Pair_Temp" (pair_address, token1_address, token2_address, routes_data)
        #         SELECT pair_address, token1_address, token2_address, routes_data FROM "Pair"
        #         ORDER BY pair_address
        #         LIMIT 1000000 OFFSET {insert_offset}
        #         ON CONFLICT DO NOTHING
        #     """)
        #     insert_offset += 1000000
        #     print("# of Moved Pairs", insert_offset)
        # Update all pairs that find routes
        cursor.execute(f"""
            UPDATE "Pair" P
            SET pair_flag = TRUE, routes_data = T.routes_data
            FROM "Pair_Temp" T
            WHERE P.pair_address = T.pair_address
        """)
        # Drop the temp table
        cursor.execute('DROP TABLE IF EXISTS "Pair_Temp"')
        connection.commit()


if __name__ == '__main__':
    main()
