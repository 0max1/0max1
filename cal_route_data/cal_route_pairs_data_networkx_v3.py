"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import os
import numpy as np
import pandas as pd
import json
import networkx as nx
from web3 import Web3
from tqdm import tqdm
from multiprocessing import Pool, Manager

def main():

    manager = Manager()
    no_routes_pairs = manager.list()
    graph = manager.dict()

    pools, pairs = load_data()

    graph = build_graph(pools, graph)

    depth_limit, blockchain_name, blockchain_id = get_params(pools)

    folder_name = generate_folder(depth_limit, pairs.shape[0], blockchain_name, blockchain_id)
    os.makedirs(folder_name, exist_ok=True)

    data = create_json(pairs, depth_limit, blockchain_name, blockchain_id, graph, no_routes_pairs)

    filename = generate_filename(data)
    with open(filename, 'w') as f:
       json.dump(data, f)

def load_data():
    pools = pd.read_csv('Allpools_tvl_test.csv')
    pairs = pd.read_csv('Allpairs-test.csv')
    return pools, pairs


def build_graph(pools, manager_graph):
    # G = nx.Graph()

    for _, row in pools.iterrows():
        token0 = row['token1_address']
        token1 = row['token2_address']

        nx.add_edge(manager_graph, token0, token1, pool_address=row['pool_address'])

    return manager_graph

def get_params(pools):
    depth_limit = 3
    blockchain_name = pools['blockchain_name'].iloc[0]
    blockchain_id = pools['blockchain_id'].iloc[0]
    return depth_limit, blockchain_name, blockchain_id


def generate_folder(depth_limit, total_pairs, blockchain_name, blockchain_id):
    generated_on = str(pd.Timestamp.now()).split(".")[0]
    generated_on = generated_on.split(" ")[0] + "_" + generated_on.split(" ")[1].replace(":", "-")
    folder_name = f"routes_data/{blockchain_name}_{blockchain_id}_depth_{depth_limit}_pairs_{total_pairs}_{generated_on}"
    return folder_name


def chunk_pairs(pairs, chunk_size=50):
    return np.array_split(pairs, pairs.shape[0] // chunk_size)


def process_chunk(pairs_chunk, depth_limit, blockchain_name, blockchain_id, chunk_index, folder_name, graph, no_routes_pairs):
    data = create_json(pairs_chunk, depth_limit, blockchain_name, blockchain_id, graph, chunk_index, no_routes_pairs)

    filename = os.path.join(folder_name, generate_filename(data, chunk_index))
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4, default=default_serializer)


def create_json(pairs, depth_limit, blockchain_name, blockchain_id, graph, chunk_index, no_routes_pairs):
    data = {
        "blockchain_name": blockchain_name,
        "blockchain_id": blockchain_id,
        "max_depth": depth_limit,
        "total_pairs": len(pairs),
        "total_pairs_find_routes": 0,
        "total_pairs_with_null_routes": 0,
        "generated_on": pd.Timestamp.now(),
        "token_pairs": []
    }

    for row in tqdm(pairs.itertuples(), total=len(pairs), desc=f"Processing pairs {chunk_index} "):
        pair_id = row.pair_id
        tokenA = row.token1_address
        tokenB = row.token2_address
        id_hash = Web3().keccak(text=tokenA + tokenB)

        pair_data = {
            "id": id_hash.hex(),
            "tokens": [tokenA, tokenB],
            "routes": {}
        }
        routes_found = False
        for i in range(1, depth_limit + 1):
            routes = get_routes(tokenA, tokenB, i, graph)
            if routes:
                pair_data["routes"][f"depth = {i}"] = routes
                routes_found = True

        if not routes_found:
            no_routes_pairs.append(pair_id)
            continue

        data["token_pairs"].append(pair_data)
    data["total_pairs_find_routes"] = len(data["token_pairs"])
    data["total_pairs_with_null_routes"] = len(pairs) - len(data["token_pairs"])

    return data


def default_serializer(o):
    if isinstance(o, np.int64):
        return int(o)
    elif isinstance(o, pd.Timestamp):
        return str(o)
    elif isinstance(o, Web3.HexBytes):
        return o.hex()
    raise TypeError(f'Object of type {o.__class__.__name__} is not JSON serializable')


def generate_filename(data, chunk_index):
    blockchain_name = data["blockchain_name"]
    blockchain_id = data["blockchain_id"]
    max_depth = data["max_depth"]
    total_pairs = data["total_pairs"]
    generated_on = str(data["generated_on"]).split(".")[0]
    generated_on = generated_on.split(" ")[0] + "_" + generated_on.split(" ")[1].replace(":", "-")
    return f"{blockchain_name}_{blockchain_id}_depth_{max_depth}_pairs_{total_pairs}_{generated_on}_chunk_{chunk_index}.json"

def get_routes(tokenA, tokenB, depth_limit, manager_graph):
    # The code is not yet perfect
    if tokenA not in manager_graph or tokenB not in manager_graph:
        # print(f"Either {tokenA} or {tokenB} not in graph. Skipping.")
        return []

    routes = []

    if tokenA not in manager_graph or tokenB not in manager_graph:
        return routes

    for path in nx.all_simple_paths(manager_graph, tokenA, tokenB, cutoff=depth_limit):

        route = {
            'depth': len(path) - 1,
            'route_id': len(routes) + 1,
            'pools': []
        }

        for i in range(len(path) - 1):
            pool = manager_graph[path[i]][path[i + 1]]['pool']

            route['pools'].append({
                'address': pool['address'],
                'tokenA': path[i],
                'tokenB': path[i + 1]
            })

        routes.append(route)

    return routes



if __name__ == '__main__':
    main()