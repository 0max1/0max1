"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import os
import numpy as np
import pandas as pd
from collections import defaultdict
import hashlib
import json
from tqdm import tqdm
from multiprocessing import Pool


def main():
    pools, pairs = load_data()

    graph = build_graph(pools, pairs)

    depth_limit, blockchain_name, blockchain_id = get_params(pools)

    folder_name = generate_folder(depth_limit, pairs.shape[0], blockchain_name, blockchain_id)
    os.makedirs(folder_name, exist_ok=True)

    chunked_pairs = chunk_pairs(pairs, chunk_size=12)
    num_processes = 4
    with Pool(processes=num_processes) as pool:
        pool.starmap(process_chunk,
                     [(pairs_chunk, depth_limit, blockchain_name, blockchain_id, i + 1, folder_name, graph) for
                      i, pairs_chunk in enumerate(chunked_pairs)])


def load_data():
    pools = pd.read_csv('Allpools_tvl_2000.csv')
    pairs = pd.read_csv('Allpairs-10.csv')
    return pools, pairs


def build_graph(pools, pairs):
    tokens = get_tokens(pairs)
    graph = init_graph(tokens)
    add_edges(graph, pools)
    return dict(graph)


def get_tokens(pairs):
    tokens = set()
    for _, row in pairs.iterrows():
        tokens.add(row['token1_address'])
        tokens.add(row['token2_address'])
    return tokens


def init_graph(tokens):
    graph = defaultdict(list)
    for token in tokens:
        graph[token] = []
    return graph


def add_edges(graph, pools):
    for _, row in pools.iterrows():
        pool_address = row['pool_address']
        token0 = row['token1_address']
        token1 = row['token2_address']
        protocol_name = row['protocol_name']

        graph[token0].append((token1, pool_address, protocol_name))
        graph[token1].append((token0, pool_address, protocol_name))


def get_params(pools):
    depth_limit = 3
    blockchain_name = pools['blockchain_name'].iloc[0]
    blockchain_id = pools['blockchain_id'].iloc[0]
    return depth_limit, blockchain_name, blockchain_id


def generate_folder(depth_limit, total_pairs, blockchain_name, blockchain_id):
    generated_on = str(pd.Timestamp.now()).split(".")[0]
    generated_on = generated_on.split(" ")[0] + "_" + generated_on.split(" ")[1].replace(":", "-")
    folder_name = f"{blockchain_name}_{blockchain_id}_depth_{depth_limit}_pairs_{total_pairs}_{generated_on}"
    return folder_name


def chunk_pairs(pairs, chunk_size=50):
    return np.array_split(pairs, pairs.shape[0] // chunk_size)


def process_chunk(pairs_chunk, depth_limit, blockchain_name, blockchain_id, chunk_index, folder_name, graph):
    chunk_data = create_json(pairs_chunk, depth_limit, blockchain_name, blockchain_id, graph, chunk_index)
    chunk_data["total_pairs"] = len(pairs_chunk)
    filename = os.path.join(folder_name, generate_filename(chunk_data, chunk_index))
    with open(filename, 'w') as f:
        json.dump(chunk_data, f, indent=4, default=default_serializer)


def create_json(pairs, depth_limit, blockchain_name, blockchain_id, graph, chunk_index):
    data = {
        "blockchain_name": blockchain_name,
        "blockchain_id": blockchain_id,
        "max_depth": depth_limit,
        "total_pairs": len(pairs),
        "generated_on": pd.Timestamp.now(),
        "token_pairs": []
    }

    for row in tqdm(pairs.itertuples(), total=len(pairs), desc=f"Processing pairs {chunk_index} "):
        tokenA = row.token1_address
        tokenB = row.token2_address

        pair_data = {
            "id": hashlib.sha256((tokenA + tokenB).encode()).hexdigest(),
            "tokens": [tokenA, tokenB],
            "routes": {}
        }

        for i in range(1, depth_limit + 1):
            routes = get_routes(tokenA, tokenB, i, graph)
            pair_data["routes"][f"depth = {i}"] = routes

        data["token_pairs"].append(pair_data)

    return data


def default_serializer(o):
    if isinstance(o, np.int64):
        return int(o)
    elif isinstance(o, pd.Timestamp):
        return str(o)
    raise TypeError(f'Object of type {o.__class__.__name__} is not JSON serializable')


def generate_filename(data, chunk_index):
    blockchain_name = data["blockchain_name"]
    blockchain_id = data["blockchain_id"]
    max_depth = data["max_depth"]
    total_pairs = data["total_pairs"]
    generated_on = str(data["generated_on"]).split(".")[0]
    generated_on = generated_on.split(" ")[0] + "_" + generated_on.split(" ")[1].replace(":", "-")
    return f"{blockchain_name}_{blockchain_id}_depth_{max_depth}_pairs_{total_pairs}_{generated_on}_chunk_{chunk_index}.json"


def get_routes(tokenA, tokenB, depth_limit, graph):
    queue = [([tokenA], [], [])]
    routes = []

    while queue:
        path, pool_addresses, protocols = queue.pop(0)
        last_token = path[-1]

        if len(path) > depth_limit:
            continue

        for next_token, pool_address, protocol in graph[last_token]:
            if next_token in path:
                continue

            new_path = path + [next_token]
            new_pool_addresses = pool_addresses + [pool_address]
            new_protocols = protocols + [protocol]

            if next_token == tokenB and len(new_path) - 1 == depth_limit:
                route = {'depth': len(new_path) - 1,
                         'route_id': len(routes) + 1,
                         'pools': [{'pool_address': new_pool_addresses[i],
                                    'token0': new_path[i],
                                    'token1': new_path[i + 1],
                                    'protocol_name': new_protocols[i]} for i in range(len(new_pool_addresses))]}
                routes.append(route)
            else:
                queue.append((new_path, new_pool_addresses, new_protocols))

    return routes


if __name__ == '__main__':
    main()