"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import os
import multiprocessing as mp
from multiprocessing.managers import ValueProxy

import numpy as np
import pandas as pd
from collections import defaultdict
import json
from web3 import Web3
from tqdm import tqdm
from multiprocessing import Pool, Manager

CAL_ROUTE_DIR = os.path.dirname(os.path.abspath(__file__))


# def main():
#     # Initialize manager for shared data between processes
#     manager = Manager()
#     no_routes_pairs = manager.list()
#
#     # Load pool and pair data
#     pools, pairs = load_data()
#
#     # Build pool graph
#     graph = build_graph(pools, pairs)
#
#     # Get blockchain parameters
#     blockchain_name, blockchain_id = get_params(pools)
#     depth_limit = 1
#
#     # Set chunk size based on depth limit
#     if depth_limit == 1:
#         chunk_size = 500
#     elif depth_limit == 2:
#         chunk_size = 100
#     elif depth_limit >= 3:
#         chunk_size = 50
#     if len(pairs) < chunk_size:
#         chunk_size = len(pairs)
#
#     # Generate output folder
#     folder_name = generate_folder(depth_limit, pairs.shape[0], blockchain_name, blockchain_id)
#     os.makedirs(folder_name, exist_ok=True)
#
#     # Split pairs into chunks
#     chunked_pairs = chunk_pairs(pairs, chunk_size)
#     num_processes = 4
#
#     # Process each chunk in parallel
#     with Pool(processes=num_processes) as pool:
#         chunk_params = [
#             (pairs_chunk, depth_limit, blockchain_name, blockchain_id, i + 1, folder_name, graph, no_routes_pairs) for
#             i, pairs_chunk in enumerate(chunked_pairs)]
#         pool.starmap(process_chunk, chunk_params)
#
#     # Save pairs without routes
#     with open(os.path.join(folder_name, f"no_routes_pairs.txt"), "w") as f:
#         f.write("\n".join(map(str, no_routes_pairs)))


def get_cal_route_dir():
    return os.path.dirname(os.path.abspath(__file__))


# def update_db_history(pairs, file_name, cursor):
#     cursor.execute('DELETE FROM "Temp"')
#     args = ','.join(cursor.mogrify('(%s)', (pair['id'],)).decode('utf-8') for pair in pairs)
#     cursor.execute('INSERT INTO "Temp" VALUES ' + args)
#     cursor.execute(f"""
#         UPDATE "Pair" SET routes_data = '{file_name}' WHERE pair_address IN (SELECT pair_address FROM "Temp")
#     """)


def create_result(data, folder_name, chunk_index, has_routes, cursor, folder_name_s):
    file_name = generate_filename(data, chunk_index)
    filename = os.path.join(folder_name, file_name)
    file_name = os.path.join(folder_name_s, file_name)
    args = ','.join(cursor.mogrify("(%s, %s, %s, %s, %s)",
                                   (pair_address[0], pair_address[1], pair_address[2], file_name, True)).decode('utf-8')
                    for pair_address in has_routes)
    cursor.execute('INSERT INTO "Pair_Temp" VALUES ' + args)
    # cursor.execute(f"""
    #             UPDATE "Pair"
    #             SET pair_flag = TRUE, routes_data = '{file_name}'
    #             WHERE pair_address IN (SELECT pair_address FROM "Temp")""")
    # cursor.execute('DELETE FROM "Temp"')
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4, default=default_serializer)


# Load pool and pair data
# def load_data():
#     # pools = pd.read_csv('Allpools_tvl_2000.csv')
#     # pairs = pd.read_csv('Allpairs-10.csv')
#     # pools = pd.read_csv('pairs_pool_data/Allpools_tvl_test.csv')
#     # pairs = pd.read_csv('pairs_pool_data/Allpairs-test.csv')
#     pool_data_path = os.path.join(CAL_ROUTE_DIR, 'pairs_pool_data', 'pool_data.csv')
#     pair_data_path = os.path.join(CAL_ROUTE_DIR, 'pairs_pool_data', 'pair_data.csv')
#     pools = pd.read_csv(pool_data_path)
#     pairs = pd.read_csv(pair_data_path)
#     return pools, pairs


# Build trading graph from pools
# def build_graph_1(pools, pairs):
#     tokens = get_tokens(pairs)
#     graph = init_graph(tokens)
#     add_edges(graph, pools)
#     # Why dict(graph)??????
#     return dict(graph)


def build_graph(pools):
    graph = defaultdict(list)
    for pool in pools:
        graph[pool[2]].append((pool[3], pool[0], pool[1]))
        graph[pool[3]].append((pool[2], pool[0], pool[1]))
    return graph


def build_temp(connection):
    with connection.cursor() as cursor:
        # cursor.execute('UPDATE "Pair" SET pair_flag = FALSE')
        print("All flags initialized")
        cursor.execute('DROP TABLE IF EXISTS "Pair_Temp"')
        cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS "Pair_Temp" (
                        "pair_address" VARCHAR(100) PRIMARY KEY,
                        "token1_address" VARCHAR(50) REFERENCES "Token"("token_address"),
                        "token2_address" VARCHAR(50) REFERENCES "Token"("token_address"),
                        "routes_data" VARCHAR(255),
                        "pair_flag" BOOLEAN DEFAULT FALSE
                    )
        """)
        connection.commit()
        print("Temp Table Created")


#
# # Get all tokens
# # Why not access database????
# def get_tokens(pairs):
#     tokens = set()
#     for _, row in pairs.iterrows():
#         tokens.add(row['token0_address'])
#         tokens.add(row['token1_address'])
#     return tokens
#
#
# # Initialize empty graph
# def init_graph(tokens):
#     graph = defaultdict(list)
#     # The code below can be rewritten to:
#     # return {token: [] for token in tokens}
#     for token in tokens:
#         graph[token] = []
#     return graph
#
#
# # Add edges to graph
# def add_edges(graph, pools):
#     for _, row in pools.iterrows():
#         pool_address = row['pool_address']
#         token0 = row['token0_address']
#         token1 = row['token1_address']
#         protocol_name = row['protocol_name']
#
#         graph[token0].append((token1, pool_address, protocol_name))
#         graph[token1].append((token0, pool_address, protocol_name))


# Get blockchain parameters
# Remember to check the type is. It should be pd.Frame
# If so, just access database
# def get_params(pools):
#     blockchain_name = pools['blockchain_name'].iloc[0]
#     # WARNING: blockchain_id may be difference. Reason Unknown!!!!!!!
#     # Possible Reason 1: caused by the change of sample data in initialization
#     # blockchain_id = pools['blockchain_id'].iloc[0]
#     return blockchain_name, 0


# Generate output folder path
def generate_folder(depth_limit, total_pairs, blockchain_name):
    results_data_path = os.path.join(CAL_ROUTE_DIR, "..", "results_data")
    generated_on = str(pd.Timestamp.now()).split(".")[0]
    generated_on = generated_on.split(" ")[0] + "_" + generated_on.split(" ")[1].replace(":", "-")
    folder_name = f"routes_data/{blockchain_name}_depth_{depth_limit}_pairs_{total_pairs}_{generated_on}"
    folder_path = os.path.join(results_data_path, folder_name)
    return folder_path, 'result/' + folder_name


# Split pairs into chunks
# return a list of numpy.ndarray
def chunk_pairs(pairs, chunk_size=50):
    return np.array_split(pairs, pairs.shape[0] // chunk_size)


# Process each chunk
# def process_chunk(pairs_chunk, depth_limit, blockchain_name, route_pairs, chunk_index, folder_name, graph,
#                   no_routes_pairs, min_tvl, min_holder):
#     data = create_json(pairs_chunk, depth_limit, blockchain_name, 0, graph, chunk_index, no_routes_pairs,
#                        min_tvl, min_holder)
#     route_pairs += data["token_pairs"]
#
#     filename = os.path.join(folder_name, generate_filename(data, chunk_index))
#     with open(filename, 'w') as f:
#         json.dump(data, f, indent=4, default=default_serializer)


def get_pair_routes(pairs, graph, depth_limit, route_pairs, chunk_index):
    for pair in tqdm(pairs, total=len(pairs), desc=f"Processing pairs {chunk_index} "):
        # for pair in pairs:
        routes = new_dfs(pair[1], pair[2], depth_limit, graph, {pair[1]})
        if len(routes) > 0:
            result = {
                "id": pair[0],
                "tokens": [pair[1], pair[2]],
                "routes_num": len(routes)
            }
            collect = []
            for d in range(depth_limit):
                collect.append([])
            for route in routes:
                new_route = {
                    "depth": len(route),
                    "route_id": len(collect[len(route) - 1]) + 1,
                    "pools": route
                }
                collect[len(route) - 1].append(new_route)
            new_routes = {}
            for i in range(depth_limit):
                # This line will only effect the view of final result, not the content, and it is a little slow
                if len(collect[depth_limit - i - 1]) > 0:
                    new_routes[f"depth = {depth_limit - i}"] = collect[depth_limit - i - 1]
            result["routes"] = new_routes
            route_pairs.append(result)


def new_dfs(current_token, target_token, depth_limit, graph, visited_tokens):
    if current_token == target_token:
        return [[]]
    if depth_limit <= 0:
        return []
    final_result = []
    for next_node in graph[current_token]:
        if next_node[0] not in visited_tokens:
            visited_tokens.add(next_node[0])
            next_results = new_dfs(next_node[0], target_token, depth_limit - 1, graph, visited_tokens)
            visited_tokens.remove(next_node[0])
            if len(next_results) > 0:
                this_path = {
                    "pool_address": next_node[1],
                    "token1": current_token,
                    "token2": next_node[0],
                    "protocol_name": next_node[2]
                }
                for result in next_results:
                    final_result.append([this_path] + result)
    return final_result


# Generate JSON data for each chunk
# def create_json(pairs, depth_limit, blockchain_name, blockchain_id, graph, chunk_index, no_routes_pairs,
#                 min_tvl, min_holder):
#     data = {
#         "blockchain_name": blockchain_name,
#         "max_depth": depth_limit,
#         "min_tvl": min_tvl,
#         "min_holder": min_holder,
#         "total_pairs": len(pairs),
#         "total_pairs_find_routes": 0,
#         "total_pairs_with_null_routes": 0,
#         "generated_on": pd.Timestamp.now(),
#         "token_pairs": []
#     }
#
#     for row in tqdm(pairs.itertuples(), total=len(pairs), desc=f"Processing pairs {chunk_index} "):
#         pair_id = row.pair_id
#         tokenA = row.token0_address
#         tokenB = row.token1_address
#         # id_hash = Web3().keccak(text=tokenA + tokenB)
#
#         pair_data = {
#             "id": pair_id,
#             "tokens": [tokenA, tokenB],
#             "routes": {}
#         }
#         routes_found = False
#         for i in range(1, depth_limit + 1):
#             # when depth = 3, everything calculated when i = 1 will be calculated again
#             # Maybe????? Need to be checked again
#             routes = get_routes(tokenA, tokenB, i, graph)
#             if routes:
#                 pair_data["routes"][f"depth = {i}"] = routes
#                 routes_found = True
#
#         if not routes_found:
#             no_routes_pairs.append([pair_id, tokenA, tokenB])
#             continue
#
#         data["token_pairs"].append(pair_data)
#     data["total_pairs_find_routes"] = len(data["token_pairs"])
#     data["total_pairs_with_null_routes"] = len(pairs) - len(data["token_pairs"])
#
#     return data


# JSON serializer
def default_serializer(o):
    if isinstance(o, np.int64):
        return int(o)
    elif isinstance(o, pd.Timestamp):
        return str(o)
    else:
        raise TypeError(f'Object of type {o.__class__.__name__} is not JSON serializable')
        # return str(o)
    # raise TypeError(f'Object of type {o.__class__.__name__} is not JSON serializable')


# Generate file name
def generate_filename(data, chunk_index):
    blockchain_name = data["blockchain_name"]
    max_depth = data["max_depth"]
    total_pairs = data["total_pairs"]
    generated_on = str(data["generated_on"]).split(".")[0]
    generated_on = generated_on.split(" ")[0] + "_" + generated_on.split(" ")[1].replace(":", "-")
    return f"{blockchain_name}_depth_{max_depth}_pairs_{total_pairs}_{generated_on}_chunk_{chunk_index}.json"

# Search to find routes
# If use record, we must check whether we have visited nodes on the given path
# def get_routes(tokenA, tokenB, depth_limit, graph):
#     if tokenA not in graph or tokenB not in graph:
#         print(f"Either {tokenA} or {tokenB} not in graph. Skipping.")
#         return []
#     routes = []
#     visited = set()
#
#     def dfs(token, path, pool_addresses, protocols, depth):
#         # check depth < depth_limit first 131
#         if token == tokenB and depth == depth_limit:
#             route = {'depth': depth,
#                      'route_id': len(routes) + 1,
#                      'pools': [{'pool_address': pool_addresses[i],
#                                 'token0': path[i],
#                                 'token1': path[i + 1],
#                                 'protocol_name': protocols[i]} for i in range(len(pool_addresses))]}
#             routes.append(route)
#             return
#
#         if depth > depth_limit:
#             return
#
#         visited.add(token)
#
#         for next_token, pool_address, protocol in graph[token]:
#             if next_token in visited:
#                 continue
#
#             dfs(next_token, path + [next_token], pool_addresses + [pool_address], protocols + [protocol], depth + 1)
#
#         visited.remove(token)
#
#     dfs(tokenA, [tokenA], [], [], 0)
#
#     return routes


# if __name__ == '__main__':
#     main()
