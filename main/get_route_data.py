"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import configparser
from multiprocessing import Pool, Manager
import web3
import pandas as pd
import numpy as np
import os
# rewrite the imports below. Too unclear
from init_mevmax_db import create_connection
# from cal_route_data import main as graph_routes_main, load_data, build_graph, get_cal_route_dir, get_tokens, init_graph, \
#     add_edges, get_params, generate_folder, chunk_pairs, process_chunk, create_json, default_serializer, \
#     generate_filename, get_routes, create_result
from cal_route_data.generate_pairs_pools_data import get_pairs, get_pool_pairs, get_blockchain_name
from cal_route_data.cal_route_pairs_data_multithreading_fix_bugs_v2_1 import build_graph, get_pair_routes, \
    generate_folder, chunk_pairs, create_result, build_temp
from init_mevmax_db.create_mevmax_db import ConfigReader
from update_mevmax_db.update_token_pair_data import update_pair_table


# 1 hour 9 min (14 million, no any database update)
# 2 hours 11 min （14132586, old update algorithm, has pair_flag initialization）
# 15 min 22 seconds (14 million, new update algorithm, no pair_flag initialization)
# 1 hour 4 min (14 million, new update algorithm, has pair_flag initialization, Update -> Insert)
# 2 hours 23 minutes (14 million, new update algorithm, has pair_flag initialization, Batch Insert, Batch commit)
# 24 minutes 53 seconds (14 million, new update algorithm, has pair_flag initialization, Batch Insert)
def main():
    # Read configuration from the INI file
    config = ConfigReader()
    user, password, host, database, port = config.getDatabaseInfo()
    connection = create_connection(user, password, host, database, port=port)
    num_holders, pairs_limit, min_tvl, depth_limit, num_processes, max_route, chunk_size = config.getRouteInfo()
    # check_set = set()
    # offset = 0
    # while offset < 720000:
    #     print("Next Turn")
    #     pairs = get_pairs(connection, num_holders, pairs_limit, offset)
    #     for pair in pairs:
    #         if pair[0] not in check_set:
    #             check_set.add(pair[0])
    #         else:
    #             print("Duplicate")
    #     offset += pairs_limit
    # exit(0)
    pools, pair_size = get_pool_pairs(connection, min_tvl, num_holders)
    build_temp(connection)
    graph = build_graph(pools)
    blockchain_name = get_blockchain_name(connection)

    # Initialize a multiprocessing manager for sharing data between processes
    manager = Manager()
    route_pairs = manager.list()

    # Determine chunk size based on depth limit and data size
    # if depth_limit == 1:
    #     chunk_size = 50000
    # elif depth_limit == 2:
    #     chunk_size = 5000
    # elif depth_limit == 3:
    #     chunk_size = 1000
    # else:
    #     chunk_size = 500

    # Generate a folder for the results
    folder_name, folder_name_s = generate_folder(depth_limit, pair_size, blockchain_name)
    os.makedirs(folder_name, exist_ok=True)

    offset = 0
    file_index = 0
    old_file_index = 0
    while offset < pair_size:
        print(f"Processing {offset} to {offset + pairs_limit}")
        pairs = get_pairs(connection, num_holders, pairs_limit, offset)
        print("Pair Search Complete")
        pairs = np.array(pairs)
        chunk_size = min(chunk_size, pair_size)
        chunked_pairs = chunk_pairs(pairs, chunk_size)
        with Pool(processes=num_processes) as pool:
            chunk_params = [(pairs_chunk, graph, depth_limit, route_pairs, i)
                            for i, pairs_chunk in enumerate(chunked_pairs)]
            pool.starmap(get_pair_routes, chunk_params)
        routes_pairs = list(route_pairs)
        route_pairs[:] = []
        current_route_num = 0
        start = 0
        has_routes = []
        with connection.cursor() as cursor:
            for i in range(len(routes_pairs)):
                has_routes.append(routes_pairs[i]['id'])
                current_route_num += routes_pairs[i]['routes_num']
                if current_route_num >= max_route or i == len(routes_pairs) - 1:
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
                    create_result(routes_results, folder_name, file_index, has_routes, cursor, folder_name_s)
                    has_routes.clear()
                    file_index += 1
                    current_route_num = 0
                    start = i + 1
            connection.commit()
        offset += pairs_limit
        print(f'Results Generated in {file_index - old_file_index} Files')
        old_file_index = file_index
    print("Updating Results to Database")
    # insert_offset = 0
    # print("Moving Pairs")
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
        cursor.execute(f"""
            UPDATE "Pair" P
            SET pair_flag = TRUE, routes_data = T.routes_data
            FROM "Pair_Temp" T
            WHERE P.pair_address = T.pair_address
        """)
        cursor.execute('DROP TABLE IF EXISTS "Pair_Temp"')
        # cursor.execute('DROP TABLE IF EXISTS "Pair"')
        # cursor.execute('ALTER TABLE "Pair_Temp" RENAME TO "Pair"')
        connection.commit()


if __name__ == '__main__':
    main()
