import pandas as pd
from collections import defaultdict
import tqdm
import json

def df_to_dict(df):
    graph = defaultdict(list)
    for _, row in df.iterrows():
        contract_id = row['pool_address']
        tokens = row[1:].dropna().values
        for token in tokens:
            if token != 0:
                connected_tokens = [(t, contract_id) for t in tokens if t != token and t != 0]
                graph[token].extend(connected_tokens)

    return graph

def bfs_paths(graph, start, end, depth_limit):
    queue = [([(start, None)], [], 0)]
    while queue:
        (path, contracts, depth) = queue.pop(0)
        for i in range(len(path)):
            if path[i][1] is None:
                if i + 1 < len(path):
                    path[i] = (path[i][0], path[i + 1][1])
                elif len(contracts) > 0:
                    path[i] = (path[i][0], contracts[0])
        (vertex, _) = path[-1]
        if depth <= depth_limit:
            for next_token, contract in graph[vertex]:
                if any(t == next_token for t, _ in path) or next_token == 0:
                    continue
                elif next_token == end:
                    yield path + [(next_token, contract)]
                else:
                    queue.append((path + [(next_token, contract)], contracts + [contract], depth + 1))

def format_paths(paths):
    formatted = []
    for path in paths:
        formatted_path = []
        for i in range(len(path) - 1):
            formatted_path.append([(path[i][0], path[i][1]), (path[-1][0], path[-1][1])])
        formatted.append(formatted_path)
    return formatted

def process_data(pair_combinations, graph, depth_limit):
    results = []
    for pair in tqdm.tqdm(pair_combinations, desc="Processing pairs"):
        start, end = pair
        paths = bfs_paths(graph, start, end, depth_limit)
        formatted_paths = format_paths(paths)
        results.extend(formatted_paths)

    return results

def create_json(results):
    output = pd.DataFrame(results)
    output = output.fillna(0)

    column1 = []
    column2 = []
    column3 = []
    column4 = []
    column5 = []
    column6 = []
    column7 = []
    column8 = []

    for row in output.iterrows():
        if row[1][1] == 0 and row[1][2] == 0:
            column1.append(row[1][0][0][0])
            column4.append(row[1][0][1][0])
            column6.append(row[1][0][1][1])

            column2.append('None')
            column3.append('None')
            column7.append('None')
            column5.append('None')
            column8.append(1)
        elif row[1][2] == 0:
            column1.append(row[1][0][0][0])
            column5.append(row[1][0][0][1])
            column2.append(row[1][1][0][0])
            column4.append(row[1][0][1][0])
            column6.append(row[1][0][1][1])

            column3.append('None')
            column7.append('None')
            column8.append(2)
        else:
            column1.append(row[1][0][0][0])
            column5.append(row[1][0][0][1])
            column2.append(row[1][1][0][0])
            column3.append(row[1][2][0][0])
            column6.append(row[1][2][0][1])
            column4.append(row[1][0][1][0])
            column7.append(row[1][2][1][1])
            column8.append(3)

    data_dict = {
        'token_1': column1,
        'token_2': column2,
        'token_3': column3,
        'token_4': column4,
        'contract_1': column5,
        'contract_2': column6,
        'contract_3': column7
    }

    route = pd.DataFrame(data_dict)
    result3 = {}
    for i in range(len(route)):
        key = (column1[i], column4[i])
        key_str = str(key)
        depth = column8[i]

        if column8[i] == 1:
            current_route = [
                {'token_address': column1[i]},
                {'token_address': column4[i]},
                {'pool_address': column6[i]}
            ]
        elif column8[i] == 2:
            current_route = [
                {'token_address': column1[i]},
                {'token_address': column2[i]},
                {'token_address': column4[i]},
                {'pool_address': column5[i]},
                {'pool_address': column6[i]}
            ]
        else:
            current_route = [
                {'token_address': column1[i]},
                {'token_address': column2[i]},
                {'token_address': column3[i]},
                {'token_address': column4[i]},
                {'pool_address': column5[i]},
                {'pool_address': column6[i]},
                {'pool_address': column7[i]}
            ]

        entry = {
            'id': len(result3.get(key_str, {}).get(depth, {'routes': []})['routes']) + 1,
            'current_route': current_route
        }

        if key_str not in result3:
            result3[key_str] = {depth: {'depth': depth, 'routes': [entry]}}
        else:
            if depth not in result3[key_str]:
                result3[key_str][depth] = {'depth': depth, 'routes': [entry]}
            else:
                result3[key_str][depth]['routes'].append(entry)

    with open('givenPair1.json', 'w') as file:
        json_result = json.dump(result3, file, indent=4)

    return json_result

def main():
    pool = pd.read_csv('Allpools-10.csv')
    df = pool.copy()
    df = df.fillna(0)
    for column in df.columns:
        if 'token' in column:
            df[column] = df[column]

    graph = df_to_dict(df)

    depth_limit = 2
    pair = pd.read_csv('Allpairs-10.csv')
    pairs = pd.DataFrame(pair)
    pair_combinations = []
    for row in pairs.iterrows():
        pair_combinations.append([row[1][1], row[1][2]])
    results = process_data(pair_combinations, graph, depth_limit)
    create_json(results)

if __name__ == '__main__':
    main()