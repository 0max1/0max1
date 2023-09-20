"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import os
import numpy as np
import pandas as pd
from collections import defaultdict
import json
from web3 import Web3
from tqdm import tqdm
from multiprocessing import Pool, Manager

CAL_ROUTE_DIR = os.path.dirname(os.path.abspath(__file__))


def main():
    # Initialize manager for shared data between processes
    manager = Manager()
    no_routes_pairs = manager.list()

    # Load pool and pair data
    pools, pairs = load_data()

    # Build pool graph
    graph = build_graph(pools, pairs)

    # Get blockchain parameters
    blockchain_name, blockchain_id = get_params(pools)
    depth_limit = 1

    # Set chunk size based on depth limit
    if depth_limit == 1:
        chunk_size = 500
    elif depth_limit == 2:
        chunk_size = 100
    elif depth_limit >= 3:
        chunk_size = 50
    if len(pairs) < chunk_size:
        chunk_size = len(pairs)

    # Generate output folder
    folder_name = generate_folder(depth_limit, pairs.shape[0], blockchain_name, blockchain_id)
    os.makedirs(folder_name, exist_ok=True)

    # Split pairs into chunks
    chunked_pairs = chunk_pairs(pairs, chunk_size)
    num_processes = 4

    # Process each chunk in parallel
    with Pool(processes=num_processes) as pool:
        chunk_params = [
            (pairs_chunk, depth_limit, blockchain_name, blockchain_id, i + 1, folder_name, graph, no_routes_pairs) for
            i, pairs_chunk in enumerate(chunked_pairs)]
        pool.starmap(process_chunk, chunk_params)

    # Save pairs without routes
    with open(os.path.join(folder_name, f"no_routes_pairs.txt"), "w") as f:
        f.write("\n".join(map(str, no_routes_pairs)))


def get_cal_route_dir():
    return os.path.dirname(os.path.abspath(__file__))


# Load pool and pair data
# Warning: if ask generate_csv_file to return pairs and pools, they must be in Dataframe to fit the other code
def load_data():
    # pools = pd.read_csv('Allpools_tvl_2000.csv')
    # pairs = pd.read_csv('Allpairs-10.csv')
    # pools = pd.read_csv('pairs_pool_data/Allpools_tvl_test.csv')
    # pairs = pd.read_csv('pairs_pool_data/Allpairs-test.csv')
    pool_data_path = os.path.join(CAL_ROUTE_DIR, 'pairs_pool_data', 'pool_data.csv')
    pair_data_path = os.path.join(CAL_ROUTE_DIR, 'pairs_pool_data', 'pair_data.csv')
    pools = pd.read_csv(pool_data_path)
    pairs = pd.read_csv(pair_data_path)
    return pools, pairs


# Build trading graph from pools
def build_graph(pools, pairs):
    tokens = get_tokens(pairs)
    graph = init_graph(tokens)
    add_edges(graph, pools)
    # Why dict(graph)??????
    return dict(graph)


# Get all tokens
# Why not access database?????
def get_tokens(pairs):
    tokens = set()
    for _, row in pairs.iterrows():
        tokens.add(row['token0_address'])
        tokens.add(row['token1_address'])
    return tokens


# Initialize empty graph
def init_graph(tokens):
    graph = defaultdict(list)
    # The code below can be rewritten to:
    # return {token: [] for token in tokens}
    for token in tokens:
        graph[token] = []
    return graph


# Add edges to graph
def add_edges(graph, pools):
    for _, row in pools.iterrows():
        pool_address = row['pool_address']
        token0 = row['token0_address']
        token1 = row['token1_address']
        protocol_name = row['protocol_name']

        graph[token0].append((token1, pool_address, protocol_name))
        graph[token1].append((token0, pool_address, protocol_name))


# Get blockchain parameters
# Remember to check the type is. It should be pd.Frame
# If so, just access database
def get_params(pools):
    blockchain_name = pools['blockchain_name'].iloc[0]
    # WARNING: blockchain_id may be difference. Reason Unknown!!!!!!!
    # Possible Reason 1: caused by the change of sample data in initialization
    # blockchain_id = pools['blockchain_id'].iloc[0]
    return blockchain_name, 0


# Generate output folder path
def generate_folder(depth_limit, total_pairs, blockchain_name, blockchain_id):
    results_data_path = os.path.join(CAL_ROUTE_DIR, "..", "results_data")
    generated_on = str(pd.Timestamp.now()).split(".")[0]
    generated_on = generated_on.split(" ")[0] + "_" + generated_on.split(" ")[1].replace(":", "-")
    folder_name = f"routes_data/{blockchain_name}_{blockchain_id}_depth_{depth_limit}_pairs_{total_pairs}_{generated_on}"
    folder_path = os.path.join(results_data_path, folder_name)
    return folder_path


# Split pairs into chunks
# return a list of numpy.ndarray
def chunk_pairs(pairs, chunk_size=50):
    return np.array_split(pairs, pairs.shape[0] // chunk_size)


# Process each chunk
def process_chunk(pairs_chunk, depth_limit, blockchain_name, blockchain_id, chunk_index, folder_name, graph,
                  no_routes_pairs, min_tvl, min_holder):
    data = create_json(pairs_chunk, depth_limit, blockchain_name, blockchain_id, graph, chunk_index, no_routes_pairs,
                       min_tvl, min_holder)

    filename = os.path.join(folder_name, generate_filename(data, chunk_index))
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4, default=default_serializer)


# Generate JSON data for each chunk
def create_json(pairs, depth_limit, blockchain_name, blockchain_id, graph, chunk_index, no_routes_pairs,
                min_tvl, min_holder):
    data = {
        "blockchain_name": blockchain_name,
        "max_depth": depth_limit,
        "min_tvl": min_tvl,
        "min_holder": min_holder,
        "total_pairs": len(pairs),
        "total_pairs_find_routes": 0,
        "total_pairs_with_null_routes": 0,
        "generated_on": pd.Timestamp.now(),
        "token_pairs": []
    }

    for row in tqdm(pairs.itertuples(), total=len(pairs), desc=f"Processing pairs {chunk_index} "):
        pair_id = row.pair_id
        tokenA = row.token0_address
        tokenB = row.token1_address
        # id_hash = Web3().keccak(text=tokenA + tokenB)

        pair_data = {
            "id": pair_id,
            "tokens": [tokenA, tokenB],
            "routes": {}
        }
        routes_found = False
        for i in range(1, depth_limit + 1):
            # when depth = 3, everything calculated when i = 1 will be calculated again
            # Maybe????? Need to be checked again
            routes = get_routes(tokenA, tokenB, i, graph)
            if routes:
                pair_data["routes"][f"depth = {i}"] = routes
                routes_found = True

        if not routes_found:
            no_routes_pairs.append([pair_id, tokenA, tokenB])
            continue

        data["token_pairs"].append(pair_data)
    data["total_pairs_find_routes"] = len(data["token_pairs"])
    data["total_pairs_with_null_routes"] = len(pairs) - len(data["token_pairs"])

    return data


# JSON serializer
def default_serializer(o):
    if isinstance(o, np.int64):
        return int(o)
    elif isinstance(o, pd.Timestamp):
        return str(o)
    elif isinstance(o, Web3.HexBytes):
        return o.hex()
    else:
        return str(o)
    # raise TypeError(f'Object of type {o.__class__.__name__} is not JSON serializable')


# Generate file name
def generate_filename(data, chunk_index):
    blockchain_name = data["blockchain_name"]
    blockchain_id = 0
    max_depth = data["max_depth"]
    total_pairs = data["total_pairs"]
    generated_on = str(data["generated_on"]).split(".")[0]
    generated_on = generated_on.split(" ")[0] + "_" + generated_on.split(" ")[1].replace(":", "-")
    return f"{blockchain_name}_{blockchain_id}_depth_{max_depth}_pairs_{total_pairs}_{generated_on}_chunk_{chunk_index}.json"


# Search to find routes
# If use record, we must check whether we have visited nodes on the given path
def get_routes(tokenA, tokenB, depth_limit, graph):
    if tokenA not in graph or tokenB not in graph:
        print(f"Either {tokenA} or {tokenB} not in graph. Skipping.")
        return []
    routes = []
    visited = set()

    def dfs(token, path, pool_addresses, protocols, depth):
        # check depth < depth_limit first 131
        if token == tokenB and depth == depth_limit:
            route = {'depth': depth,
                     'route_id': len(routes) + 1,
                     'pools': [{'pool_address': pool_addresses[i],
                                'token0': path[i],
                                'token1': path[i + 1],
                                'protocol_name': protocols[i]} for i in range(len(pool_addresses))]}
            routes.append(route)
            return

        if depth > depth_limit:
            return

        visited.add(token)

        for next_token, pool_address, protocol in graph[token]:
            if next_token in visited:
                continue

            dfs(next_token, path + [next_token], pool_addresses + [pool_address], protocols + [protocol], depth + 1)

        visited.remove(token)

    dfs(tokenA, [tokenA], [], [], 0)

    return routes


if __name__ == '__main__':
    main()
