"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import os
from datetime import datetime
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from tqdm import tqdm
from .convert_csv_to_json import *
import pandas as pd

# Define the directory where this script is located
CAL_ROUTE_DIR = os.path.dirname(os.path.abspath(__file__))

def get_cal_route_dir():
    return os.path.dirname(os.path.abspath(__file__))

# This function fetches and processes liquidity pool data ordered by total value locked in USD.
def fetch_and_process_liquidity_pools(url, name, num_queries):
    # Set up the GraphQL client
    transport = RequestsHTTPTransport(url=url, verify=True, retries=3)
    client = Client(transport=transport)

    all_pools = []

    # Fetch data in batches
    for i in tqdm(range(num_queries), desc=f"Fetching {name} pools..."):
        query = gql(f'''
          query {{
            liquidityPools(orderBy: totalValueLockedUSD, orderDirection: desc, first: 1000, skip: {i * 1000}) {{
              id totalValueLockedUSD fees {{ feePercentage }} inputTokens {{ id symbol decimals }}
            }}
          }}
        ''')

        response = client.execute(query)

        # Normalize JSON response into DataFrames
        df1 = pd.json_normalize(response['liquidityPools'], max_level=1)
        df2 = pd.json_normalize(df1['inputTokens'].apply(pd.Series)[0])
        df3 = pd.json_normalize(df1['inputTokens'].apply(pd.Series)[1])
        df4 = pd.json_normalize(df1['fees'].apply(pd.Series)[0])

        # Concatenate DataFrames
        pools = pd.concat([df1[['id', 'totalValueLockedUSD']], df4, df2, df3], axis=1)
        all_pools.append(pools)

    result = pd.concat(all_pools, ignore_index=True)
    result.columns = ['pool_address', 'tvl', 'fee',
                      'token1', 'token1_symbol', 'token1_decimals',
                      'token2', 'token2_symbol', 'token2_decimals']
    result.insert(0, 'Name', name)

    return result

# This function fetches data based on a query and processes it using a handler function.
def fetch_and_handle_data_by_query(url, handler, name):
    # Determine the appropriate query based on the name
    if name == "thena_fusion":
        query = '''
            query {
              pools(orderBy: totalValueLockedUSD, orderDirection: desc, first: 1000) {
                id totalValueLockedUSD fee
                token0 { id symbol decimals }
                token1 { id symbol decimals }
              }
            }
            '''
    elif name == "thena_v1":
        query = '''
            query {
              pairs(first: 1000, orderBy: reserveUSD, orderDirection: desc) {
                id reserveUSD isStable
                token0 { id symbol decimals }
                token1 { id symbol decimals }
              }
            }
            '''
    print(f"Fetching {name} pools...")
    transport = RequestsHTTPTransport(url=url, verify=True, retries=3)
    client = Client(transport=transport)

    gql_query = gql(query)
    response = client.execute(gql_query)

    main_key = 'pools' if 'pools' in response else 'pairs'

    df = handler(response[main_key])
    Name = pd.Series([name for x in range(len(df.index))])
    df.insert(loc=0, column='Name', value=Name)

    return df

# Process data for the thena_fusion pool
def process_thena_fusion_data(data):
    df = pd.json_normalize(data, max_level=1)
    df.columns = ['pool_address', 'tvl', 'fee',
                  'token1', 'token1_symbol', 'token1_decimals',
                  'token2', 'token2_symbol', 'token2_decimals']
    df['fee'] = df['fee'].apply(int) / 10000
    return df

# Process data for the thena_v1 pool
def process_thena_v1_data(data):
    df = pd.json_normalize(data, max_level=1)
    df.columns = ['pool_address', 'tvl', 'isStable',
                  'token1', 'token1_symbol', 'token1_decimals',
                  'token2', 'token2_symbol', 'token2_decimals']
    df['fee'] = df['isStable'].apply(lambda x: 0.01 if x else 0.2)
    df = df.drop('isStable', axis=1)
    return df

# Fetch data by pool name and order
def fetch_data_by_pool_name_and_order(pool_name, url, order_by, fee):
    print(f"Fetching {pool_name} pools...")
    transport = RequestsHTTPTransport(
        url=url,
        verify=True,
        retries=3
    )

    client = Client(transport=transport)
    if pool_name != "babydoge":
        query = gql(f'''
        query {{
          pairs(first: 1000, orderBy: {order_by}, orderDirection: desc) {{
            id
            token0 {{ id symbol decimals }}
            token1 {{ id symbol decimals }}
          }}
        }}
        ''')
    else:
        query = gql('''
        query MyQuery {
          pairs {
            id
            token0 { id symbol decimals }
            token1 { id symbol decimals }
          }
        }
        ''')

    response = client.execute(query)
    pool_data = pd.json_normalize(response['pairs'], max_level=1)

    pool_data.columns = ['pool_address',
                         'token1', 'token1_symbol', 'token1_decimals',
                         'token2', 'token2_symbol', 'token2_decimals']

    Name = pd.Series([pool_name for _ in range(len(pool_data.index))])
    pool_data.insert(loc=0, column='Name', value=Name)
    pool_data.insert(loc=2, column='fee', value=fee)

    return pool_data

def main():
    # Fetch and process liquidity pool data
    apeswap_pools = fetch_and_process_liquidity_pools('https://api.thegraph.com/subgraphs/name/messari/apeswap-bsc',
                                                      'apeswap', 5)
    biswap_pools = fetch_and_process_liquidity_pools('https://api.thegraph.com/subgraphs/name/messari/biswap-bsc',
                                                     'biswap', 3)
    ellipsis_pools = fetch_and_process_liquidity_pools(
        'https://api.thegraph.com/subgraphs/name/messari/ellipsis-finance-bsc', 'ellipsis', 1)
    pancakeswap_pools = fetch_and_process_liquidity_pools(
        'https://api.thegraph.com/subgraphs/name/messari/pancakeswap-v3-bsc', 'pancakeswap', 6)
    sushiswap_pools = fetch_and_process_liquidity_pools('https://api.thegraph.com/subgraphs/name/messari/sushiswap-bsc',
                                                        'sushiswap', 2)
    sushiswapv3_pools = fetch_and_process_liquidity_pools(
        'https://api.thegraph.com/subgraphs/name/messari/sushiswap-v3-bsc', 'sushiswapv3', 1)
    uniswap_pools = fetch_and_process_liquidity_pools('https://api.thegraph.com/subgraphs/name/messari/uniswap-v3-bsc',
                                                      'uniswap', 2)

    # Fetch and handle specific data using queries and handlers
    thena_fusion_pools = fetch_and_handle_data_by_query(
        'https://api.thegraph.com/subgraphs/name/thenaursa/thena-fusion', process_thena_fusion_data, 'thena_fusion')
    thena_v1_pools = fetch_and_handle_data_by_query('https://api.thegraph.com/subgraphs/name/thenaursa/thena-v1',
                                                    process_thena_v1_data, 'thena_v1')

    # Fetch data by pool name and order
    knightswap_pools = fetch_data_by_pool_name_and_order('knightswap',
                                                         'https://api.thegraph.com/subgraphs/name/unchase/knightswap-bsc',
                                                         'syncAtTimestamp', 0.2)
    nomiswap_pools = fetch_data_by_pool_name_and_order('nomiswap',
                                                       'https://api.thegraph.com/subgraphs/name/unchase/nomiswap-bsc',
                                                       'syncAtTimestamp', 0.1)
    babydoge_pools = fetch_data_by_pool_name_and_order('babydoge',
                                                       'https://api.thegraph.com/subgraphs/name/unchase/babydoge-bsc',
                                                       'Null', 0.3)
    alitaswap_pools = fetch_data_by_pool_name_and_order('alitaswap',
                                                        'https://api.thegraph.com/subgraphs/name/unchase/alitaswap-bsc',
                                                        'syncAtTimestamp', 0.1)
    appleswap_pools = fetch_data_by_pool_name_and_order('appleswap',
                                                        'https://api.thegraph.com/subgraphs/name/thinhpn/appleswap-bsc-chain',
                                                        'createdAtTimestamp', 0.1)
    cafeswap_pools = fetch_data_by_pool_name_and_order('cafeswap',
                                                       'https://api.thegraph.com/subgraphs/name/unchase/cafeswap-bsc',
                                                       'createdAtTimestamp', 0.1)
    cheeseswap_pools = fetch_data_by_pool_name_and_order('cheeseswap',
                                                         'https://api.thegraph.com/subgraphs/name/unchase/cheeseswap-bsc',
                                                         'createdAtTimestamp', 0.1)
    coinswap_pools = fetch_data_by_pool_name_and_order('coinswap',
                                                       'https://api.thegraph.com/subgraphs/name/unchase/coinswap-bsc',
                                                       'createdAtTimestamp', 0.1)

    # Create directories for output files
    pool_directory = os.path.join(CAL_ROUTE_DIR, 'original_data')
    if not os.path.exists(pool_directory):
        os.makedirs(pool_directory)

    token_directory = os.path.join(CAL_ROUTE_DIR, 'original_data')
    if not os.path.exists(token_directory):
        os.makedirs(token_directory)

    # Get current timestamp
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    # Concatenate all pool data
    all_pools = pd.concat(
        [apeswap_pools, biswap_pools, ellipsis_pools, pancakeswap_pools, sushiswap_pools, sushiswapv3_pools,
         uniswap_pools, thena_fusion_pools, thena_v1_pools, knightswap_pools, nomiswap_pools, babydoge_pools,
         alitaswap_pools, appleswap_pools, cafeswap_pools, cheeseswap_pools, coinswap_pools], ignore_index=True)

    # all_pools = pd.concat(
    #     [apeswap_pools], ignore_index=True)
    # Convert data types and calculations
    all_pools['tvl'] = all_pools['tvl'].fillna(0)
    all_pools['tvl'] = all_pools['tvl'].apply(float)
    all_pools['fee'] = all_pools['fee'].apply(float)
    all_pools['fee'] = all_pools['fee'].div(100)

    # Sort and filter pool data
    # token_pool = all_pools.sort_values(by=['tvl'], ascending=False)
    # token_pool = token_pool.head(500)
    token_pool = all_pools

    # Extract unique tokens
    token1 = token_pool[['token1_symbol', 'token1', 'token1_decimals']].drop_duplicates()
    token1.columns = [['token_symbol', 'token_address', 'token_decimals']]
    token1['holder'] = 0
    token2 = token_pool[['token2_symbol', 'token2', 'token2_decimals']].drop_duplicates()
    token2.columns = [['token_symbol', 'token_address', 'token_decimals']]
    token2['holder'] = 0
    tokens = pd.concat([token1, token2], ignore_index=True)
    tokens.columns = ['symbol', 'token', 'decimals', 'holder']
    tokens = tokens.drop_duplicates(subset='token')

    # Write output files
    print("Writing pool output...")
    pool_csv_name = os.path.join(pool_directory, f"pool_{timestamp}.csv")
    token_csv_name = os.path.join(token_directory, f"token_{timestamp}.csv")
    all_pools.to_csv(pool_csv_name)
    tokens.to_csv(token_csv_name)

    output_directory = pool_directory
    pool_convert_csv_to_json(pool_csv_name, output_directory, CAL_ROUTE_DIR)
    token_convert_csv_to_json(token_csv_name, output_directory, CAL_ROUTE_DIR)
    print("Done!")


if __name__ == '__main__':
    main()