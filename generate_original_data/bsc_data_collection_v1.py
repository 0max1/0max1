"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import pandas as pd

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

from numpy import float32
from datetime import datetime

import os

timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

# Specify the directory path
token_directory = "/home/ec2-user/subgraph/bsc/token"
pool_directory = "/home/ec2-user/subgraph/bsc/pool"
# contract_directory = "/home/ec2-user/bsc/contract"
route_directory = "/home/ec2-user/subgraph/bsc/route"

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/messari/apeswap-bsc',
    verify=True,
    retries=3)

client = Client(
    transport=transport)

# First 1000 pool
query1 = gql('''
query {
        liquidityPools(orderBy: totalValueLockedUSD,
                                orderDirection: desc,
                                first: 1000){
            id
            totalValueLockedUSD
            fees {
              feePercentage
            }
            inputTokens {
              id
              symbol
              decimals
    }
  }
      }
''')

response1 = client.execute(query1)

df11 = pd.json_normalize(response1['liquidityPools'], max_level=1)
df12 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[0])
df13 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[1])
df14 = pd.json_normalize(df11['fees'].apply(pd.Series)[2])

apeswap_pool1 = pd.concat([df11[['id', 'totalValueLockedUSD']], df14, df12, df13], axis=1)

# pool 1001-2000
query2 = gql('''
query {
        liquidityPools(orderBy: totalValueLockedUSD,
                                orderDirection: desc,
                                first: 1000,
                                skip: 1000){
            id
            totalValueLockedUSD
            fees {
              feePercentage
            }
            inputTokens {
              id
              symbol
              decimals
    }
  }
      }
''')

response2 = client.execute(query2)

df21 = pd.json_normalize(response2['liquidityPools'], max_level=1)
df22 = pd.json_normalize(df21['inputTokens'].apply(pd.Series)[0])
df23 = pd.json_normalize(df21['inputTokens'].apply(pd.Series)[1])
df24 = pd.json_normalize(df21['fees'].apply(pd.Series)[2])

apeswap_pool2 = pd.concat([df21[['id', 'totalValueLockedUSD']], df24, df22, df23], axis=1)

# pool 2001-3000
query3 = gql('''
query {
        liquidityPools(orderBy: totalValueLockedUSD,
                                orderDirection: desc,
                                first: 1000,
                                skip: 2000){
            id
            totalValueLockedUSD
            fees {
              feePercentage
            }
            inputTokens {
              id
              symbol
              decimals
    }
  }
      }
''')

response3 = client.execute(query3)

df31 = pd.json_normalize(response3['liquidityPools'], max_level=1)
df32 = pd.json_normalize(df31['inputTokens'].apply(pd.Series)[0])
df33 = pd.json_normalize(df31['inputTokens'].apply(pd.Series)[1])
df34 = pd.json_normalize(df31['fees'].apply(pd.Series)[2])

apeswap_pool3 = pd.concat([df31[['id', 'totalValueLockedUSD']], df34, df32, df33], axis=1)

# pool 3001-4000
query4 = gql('''
query {
        liquidityPools(orderBy: totalValueLockedUSD,
                                orderDirection: desc,
                                first: 1000,
                                skip: 3000){
            id
            totalValueLockedUSD
            fees {
              feePercentage
            }
            inputTokens {
              id
              symbol
              decimals
    }
  }
      }
''')

response4 = client.execute(query4)

df41 = pd.json_normalize(response4['liquidityPools'], max_level=1)
df42 = pd.json_normalize(df41['inputTokens'].apply(pd.Series)[0])
df43 = pd.json_normalize(df41['inputTokens'].apply(pd.Series)[1])
df44 = pd.json_normalize(df41['fees'].apply(pd.Series)[2])

apeswap_pool4 = pd.concat([df41[['id', 'totalValueLockedUSD']], df44, df42, df43], axis=1)

# pool 4001-5000
query5 = gql('''
query {
        liquidityPools(orderBy: totalValueLockedUSD,
                                orderDirection: desc,
                                first: 1000,
                                skip: 4000){
            id
            totalValueLockedUSD
            fees {
              feePercentage
            }
            inputTokens {
              id
              symbol
              decimals
    }
  }
      }
''')

response5 = client.execute(query5)

df51 = pd.json_normalize(response5['liquidityPools'], max_level=1)
df52 = pd.json_normalize(df51['inputTokens'].apply(pd.Series)[0])
df53 = pd.json_normalize(df51['inputTokens'].apply(pd.Series)[1])
df54 = pd.json_normalize(df51['fees'].apply(pd.Series)[2])

apeswap_pool5 = pd.concat([df51[['id', 'totalValueLockedUSD']], df54, df52, df53], axis=1)

# Combination
apeswap_pools = pd.concat([apeswap_pool1, apeswap_pool2, apeswap_pool3, apeswap_pool4, apeswap_pool5],
                          ignore_index=True)
apeswap_pools.columns = ['pool_address', 'tvl', 'fee',
                         'token1', 'token1_symbol', 'token1_decimals',
                         'token2', 'token2_symbol', 'token2_decimals']
Name = pd.Series(["apeswap" for x in range(len(apeswap_pools.index))])
apeswap_pools.insert(loc=0, column='Name', value=Name)

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/messari/biswap-bsc',
    verify=True,
    retries=3)

client = Client(
    transport=transport)

# First 1000 pool
query1 = gql('''
query {
        liquidityPools(orderBy: totalValueLockedUSD,
                                orderDirection: desc,
                                first: 1000){
            id
            totalValueLockedUSD
            fees {
              feePercentage
            }
            inputTokens {
              id
              symbol
              decimals
    }
  }
      }
''')

response1 = client.execute(query1)

df11 = pd.json_normalize(response1['liquidityPools'], max_level=1)
df12 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[0])
df13 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[1])
df14 = pd.json_normalize(df11['fees'].apply(pd.Series)[2])

biswap_pool1 = pd.concat([df11[['id', 'totalValueLockedUSD']], df14, df12, df13], axis=1)

# pool 1001-2000
query2 = gql('''
query {
        liquidityPools(orderBy: totalValueLockedUSD,
                                orderDirection: desc,
                                first: 1000,
                                skip: 1000){
            id
            totalValueLockedUSD
            fees {
              feePercentage
            }
            inputTokens {
              id
              symbol
              decimals
    }
  }
      }
''')

response2 = client.execute(query2)

df21 = pd.json_normalize(response2['liquidityPools'], max_level=1)
df22 = pd.json_normalize(df21['inputTokens'].apply(pd.Series)[0])
df23 = pd.json_normalize(df21['inputTokens'].apply(pd.Series)[1])
df24 = pd.json_normalize(df21['fees'].apply(pd.Series)[2])

biswap_pool2 = pd.concat([df21[['id', 'totalValueLockedUSD']], df24, df22, df23], axis=1)

# pool 2001-3000
query3 = gql('''
query {
        liquidityPools(orderBy: totalValueLockedUSD,
                                orderDirection: desc,
                                first: 1000,
                                skip: 2000){
            id
            totalValueLockedUSD
            fees {
              feePercentage
            }
            inputTokens {
              id
              symbol
              decimals
    }
  }
      }
''')

response3 = client.execute(query3)

df31 = pd.json_normalize(response3['liquidityPools'], max_level=1)
df32 = pd.json_normalize(df31['inputTokens'].apply(pd.Series)[0])
df33 = pd.json_normalize(df31['inputTokens'].apply(pd.Series)[1])
df34 = pd.json_normalize(df31['fees'].apply(pd.Series)[2])

biswap_pool3 = pd.concat([df31[['id', 'totalValueLockedUSD']], df34, df32, df33], axis=1)

# Combination
biswap_pools = pd.concat([biswap_pool1, biswap_pool2, biswap_pool3], ignore_index=True)
biswap_pools.columns = ['pool_address', 'tvl', 'fee',
                        'token1', 'token1_symbol', 'token1_decimals',
                        'token2', 'token2_symbol', 'token2_decimals']
Name = pd.Series(["biswap" for x in range(len(biswap_pools.index))])
biswap_pools.insert(loc=0, column='Name', value=Name)

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/messari/ellipsis-finance-bsc',
    verify=True,
    retries=3)

client = Client(
    transport=transport)

# First 1000 pool
query1 = gql('''
query {
        liquidityPools(orderBy: totalValueLockedUSD,
                                orderDirection: desc,
                                first: 1000){
            id
            totalValueLockedUSD
            fees {
              feePercentage
            }
            inputTokens {
              id
              symbol
              decimals
    }
  }
      }
''')

response1 = client.execute(query1)

df11 = pd.json_normalize(response1['liquidityPools'], max_level=1)
df12 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[0])
df13 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[1])
df14 = pd.json_normalize(df11['fees'].apply(pd.Series)[2])

ellipsis_pool1 = pd.concat([df11[['id', 'totalValueLockedUSD']], df14, df12, df13], axis=1)

# Combination
ellipsis_pools = pd.concat([ellipsis_pool1], ignore_index=True)
ellipsis_pools.columns = ['pool_address', 'tvl', 'fee',
                          'token1', 'token1_symbol', 'token1_decimals',
                          'token2', 'token2_symbol', 'token2_decimals']
Name = pd.Series(["ellipsis" for x in range(len(biswap_pools.index))])
ellipsis_pools.insert(loc=0, column='Name', value=Name)

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/messari/pancakeswap-v3-bsc',
    verify=True,
    retries=3)

client = Client(
    transport=transport)

# First 1000 pool
query1 = gql('''
query {
        liquidityPools(orderBy: totalValueLockedUSD,
                                orderDirection: desc,
                                first: 1000){
            id
            totalValueLockedUSD
            fees {
              feePercentage
            }
            inputTokens {
              id
              symbol
              decimals
    }
  }
      }
''')

response1 = client.execute(query1)

df11 = pd.json_normalize(response1['liquidityPools'], max_level=1)
df12 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[0])
df13 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[1])
df14 = pd.json_normalize(df11['fees'].apply(pd.Series)[2])

pancakeswap_pool1 = pd.concat([df11[['id', 'totalValueLockedUSD']], df14, df12, df13], axis=1)

# pool 1001-2000
query2 = gql('''
query {
        liquidityPools(orderBy: totalValueLockedUSD,
                                orderDirection: desc,
                                first: 1000,
                                skip: 1000){
            id
            totalValueLockedUSD
            fees {
              feePercentage
            }
            inputTokens {
              id
              symbol
              decimals
    }
  }
      }
''')

response2 = client.execute(query2)

df21 = pd.json_normalize(response2['liquidityPools'], max_level=1)
df22 = pd.json_normalize(df21['inputTokens'].apply(pd.Series)[0])
df23 = pd.json_normalize(df21['inputTokens'].apply(pd.Series)[1])
df24 = pd.json_normalize(df21['fees'].apply(pd.Series)[2])

pancakeswap_pool2 = pd.concat([df21[['id', 'totalValueLockedUSD']], df24, df22, df23], axis=1)

# pool 2001-3000
query3 = gql('''
query {
        liquidityPools(orderBy: totalValueLockedUSD,
                                orderDirection: desc,
                                first: 1000,
                                skip: 2000){
            id
            totalValueLockedUSD
            fees {
              feePercentage
            }
            inputTokens {
              id
              symbol
              decimals
    }
  }
      }
''')

response3 = client.execute(query3)

df31 = pd.json_normalize(response3['liquidityPools'], max_level=1)
df32 = pd.json_normalize(df31['inputTokens'].apply(pd.Series)[0])
df33 = pd.json_normalize(df31['inputTokens'].apply(pd.Series)[1])
df34 = pd.json_normalize(df31['fees'].apply(pd.Series)[2])

pancakeswap_pool3 = pd.concat([df31[['id', 'totalValueLockedUSD']], df34, df32, df33], axis=1)

# pool 3001-4000
query4 = gql('''
query {
        liquidityPools(orderBy: totalValueLockedUSD,
                                orderDirection: desc,
                                first: 1000,
                                skip: 3000){
            id
            totalValueLockedUSD
            fees {
              feePercentage
            }
            inputTokens {
              id
              symbol
              decimals
    }
  }
      }
''')

response4 = client.execute(query4)

df41 = pd.json_normalize(response4['liquidityPools'], max_level=1)
df42 = pd.json_normalize(df41['inputTokens'].apply(pd.Series)[0])
df43 = pd.json_normalize(df41['inputTokens'].apply(pd.Series)[1])
df44 = pd.json_normalize(df41['fees'].apply(pd.Series)[2])

pancakeswap_pool4 = pd.concat([df41[['id', 'totalValueLockedUSD']], df44, df42, df43], axis=1)

# pool 4001-5000
query5 = gql('''
query {
        liquidityPools(orderBy: totalValueLockedUSD,
                                orderDirection: desc,
                                first: 1000,
                                skip: 4000){
            id
            totalValueLockedUSD
            fees {
              feePercentage
            }
            inputTokens {
              id
              symbol
              decimals
    }
  }
      }
''')

response5 = client.execute(query5)

df51 = pd.json_normalize(response5['liquidityPools'], max_level=1)
df52 = pd.json_normalize(df51['inputTokens'].apply(pd.Series)[0])
df53 = pd.json_normalize(df51['inputTokens'].apply(pd.Series)[1])
df54 = pd.json_normalize(df51['fees'].apply(pd.Series)[2])

pancakeswap_pool5 = pd.concat([df51[['id', 'totalValueLockedUSD']], df54, df52, df53], axis=1)

# pool 5001-6000
query6 = gql('''
query {
        liquidityPools(orderBy: totalValueLockedUSD,
                                orderDirection: desc,
                                first: 1000,
                                skip: 5000){
            id
            totalValueLockedUSD
            fees {
              feePercentage
            }
            inputTokens {
              id
              symbol
              decimals
    }
  }
      }
''')

response6 = client.execute(query6)

df61 = pd.json_normalize(response6['liquidityPools'], max_level=1)
df62 = pd.json_normalize(df61['inputTokens'].apply(pd.Series)[0])
df63 = pd.json_normalize(df61['inputTokens'].apply(pd.Series)[1])
df64 = pd.json_normalize(df61['fees'].apply(pd.Series)[2])

pancakeswap_pool6 = pd.concat([df61[['id', 'totalValueLockedUSD']], df64, df62, df63], axis=1)

# Combination
pancakeswap_pools = pd.concat(
    [pancakeswap_pool1, pancakeswap_pool2, pancakeswap_pool3, pancakeswap_pool4, pancakeswap_pool5, pancakeswap_pool6],
    ignore_index=True)
pancakeswap_pools.columns = ['pool_address', 'tvl', 'fee',
                             'token1', 'token1_symbol', 'token1_decimals',
                             'token2', 'token2_symbol', 'token2_decimals']
Name = pd.Series(["pancakeswap" for x in range(len(pancakeswap_pools.index))])
pancakeswap_pools.insert(loc=0, column='Name', value=Name)

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/messari/sushiswap-bsc',
    verify=True,
    retries=3)

client = Client(
    transport=transport)

# First 1000 pool
query1 = gql('''
query {
        liquidityPools(orderBy: totalValueLockedUSD,
                                orderDirection: desc,
                                first: 1000){
            id
            totalValueLockedUSD
            fees {
              feePercentage
            }
            inputTokens {
              id
              symbol
              decimals
    }
  }
      }
''')

response1 = client.execute(query1)

df11 = pd.json_normalize(response1['liquidityPools'], max_level=1)
df12 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[0])
df13 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[1])
df14 = pd.json_normalize(df11['fees'].apply(pd.Series)[2])

sushiswap_pool1 = pd.concat([df11[['id', 'totalValueLockedUSD']], df14, df12, df13], axis=1)

# pool 1001-2000
query2 = gql('''
query {
        liquidityPools(orderBy: totalValueLockedUSD,
                                orderDirection: desc,
                                first: 1000,
                                skip: 1000){
            id
            totalValueLockedUSD
            fees {
              feePercentage
            }
            inputTokens {
              id
              symbol
              decimals
    }
  }
      }
''')

response2 = client.execute(query2)

df21 = pd.json_normalize(response2['liquidityPools'], max_level=1)
df22 = pd.json_normalize(df21['inputTokens'].apply(pd.Series)[0])
df23 = pd.json_normalize(df21['inputTokens'].apply(pd.Series)[1])
df24 = pd.json_normalize(df21['fees'].apply(pd.Series)[2])

sushiswap_pool2 = pd.concat([df21[['id', 'totalValueLockedUSD']], df24, df22, df23], axis=1)

# Combination
sushiswap_pools = pd.concat([sushiswap_pool1, sushiswap_pool2], ignore_index=True)
sushiswap_pools.columns = ['pool_address', 'tvl', 'fee',
                           'token1', 'token1_symbol', 'token1_decimals',
                           'token2', 'token2_symbol', 'token2_decimals']
Name = pd.Series(["sushiswap" for x in range(len(sushiswap_pools.index))])
sushiswap_pools.insert(loc=0, column='Name', value=Name)

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/messari/sushiswap-v3-bsc',
    verify=True,
    retries=3)

client = Client(
    transport=transport)

# First 1000 pool
query1 = gql('''
query {
        liquidityPools(orderBy: totalValueLockedUSD,
                                orderDirection: desc,
                                first: 1000){
            id
            totalValueLockedUSD
            fees {
              feePercentage
            }
            inputTokens {
              id
              symbol
              decimals
    }
  }
      }
''')

response1 = client.execute(query1)

df11 = pd.json_normalize(response1['liquidityPools'], max_level=1)
df12 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[0])
df13 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[1])
df14 = pd.json_normalize(df11['fees'].apply(pd.Series)[2])

sushiswapv3_pool1 = pd.concat([df11[['id', 'totalValueLockedUSD']], df14, df12, df13], axis=1)

# Combination
sushiswapv3_pools = pd.concat([sushiswapv3_pool1], ignore_index=True)
sushiswapv3_pools.columns = ['pool_address', 'tvl', 'fee',
                             'token1', 'token1_symbol', 'token1_decimals',
                             'token2', 'token2_symbol', 'token2_decimals']
Name = pd.Series(["sushiswapv3" for x in range(len(sushiswapv3_pools.index))])
sushiswapv3_pools.insert(loc=0, column='Name', value=Name)

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/messari/uniswap-v3-bsc',
    verify=True,
    retries=3)

client = Client(
    transport=transport)

# First 1000 pool
query1 = gql('''
query {
        liquidityPools(orderBy: totalValueLockedUSD,
                                orderDirection: desc,
                                first: 1000){
            id
            totalValueLockedUSD
            fees {
              feePercentage
            }
            inputTokens {
              id
              symbol
              decimals
    }
  }
      }
''')

response1 = client.execute(query1)

df11 = pd.json_normalize(response1['liquidityPools'], max_level=1)
df12 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[0])
df13 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[1])
df14 = pd.json_normalize(df11['fees'].apply(pd.Series)[2])

uniswap_pool1 = pd.concat([df11[['id', 'totalValueLockedUSD']], df14, df12, df13], axis=1)

# pool 1001-2000
query2 = gql('''
query {
        liquidityPools(orderBy: totalValueLockedUSD,
                                orderDirection: desc,
                                first: 1000,
                                skip: 1000){
            id
            totalValueLockedUSD
            fees {
              feePercentage
            }
            inputTokens {
              id
              symbol
              decimals
    }
  }
      }
''')

response2 = client.execute(query2)

df21 = pd.json_normalize(response2['liquidityPools'], max_level=1)
df22 = pd.json_normalize(df21['inputTokens'].apply(pd.Series)[0])
df23 = pd.json_normalize(df21['inputTokens'].apply(pd.Series)[1])
df24 = pd.json_normalize(df21['fees'].apply(pd.Series)[2])

uniswap_pool2 = pd.concat([df21[['id', 'totalValueLockedUSD']], df24, df22, df23], axis=1)

# Combination
uniswap_pools = pd.concat([uniswap_pool1, uniswap_pool2], ignore_index=True)
uniswap_pools.columns = ['pool_address', 'tvl', 'fee',
                         'token1', 'token1_symbol', 'token1_decimals',
                         'token2', 'token2_symbol', 'token2_decimals']
Name = pd.Series(["uniswap" for x in range(len(uniswap_pools.index))])
uniswap_pools.insert(loc=0, column='Name', value=Name)

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/thenaursa/thena-fusion',
    verify=True,
    retries=3)

client = Client(
    transport=transport)

# First 1000 pool
query1 = gql('''
query {
  pools(orderBy: totalValueLockedUSD,
        orderDirection: desc,
        first: 1000) {
    id
    totalValueLockedUSD
    fee
    token0 {
      id
      symbol
      decimals
    }
    token1 {
      id
      symbol
      decimals
    }
  }
}
''')

response1 = client.execute(query1)

thena_fusion_pool1 = pd.json_normalize(response1['pools'], max_level=1)

# Combination
thena_fusion_pools = pd.concat([thena_fusion_pool1], ignore_index=True)
thena_fusion_pools.columns = ['pool_address', 'tvl', 'fee',
                              'token1', 'token1_symbol', 'token1_decimals',
                              'token2', 'token2_symbol', 'token2_decimals']
Name = pd.Series(["thena_fusion" for x in range(len(thena_fusion_pools.index))])
thena_fusion_pools.insert(loc=0, column='Name', value=Name)
thena_fusion_pools['fee'] = thena_fusion_pools['fee'].apply(int) / 10000

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/thenaursa/thena-v1',
    verify=True,
    retries=3)

client = Client(
    transport=transport)

# First 1000 pool
query1 = gql('''
query {
  pairs(first: 1000, orderBy: reserveUSD, orderDirection: desc) {
    id
    reserveUSD
    isStable
    token0 {
      id
      symbol
      decimals
    }
    token1 {
      id
      symbol
      decimals
    }
  }
}
''')

response1 = client.execute(query1)

thena_v1_pool1 = pd.json_normalize(response1['pairs'], max_level=1)

# Combination
thena_v1_pools = pd.concat([thena_v1_pool1], ignore_index=True)
thena_v1_pools.columns = ['pool_address', 'tvl', 'isStable',
                          'token1', 'token1_symbol', 'token1_decimals',
                          'token2', 'token2_symbol', 'token2_decimals']
thena_v1_pools['fee'] = thena_v1_pools['isStable'].apply(lambda x: 0.01 if x else 0.2)
columns = thena_v1_pools.columns.tolist()
columns.remove('fee')
columns.insert(3, 'fee')
thena_v1_pools = thena_v1_pools[columns]
Name = pd.Series(["thena_v1" for x in range(len(thena_v1_pools.index))])
thena_v1_pools.insert(loc=0, column='Name', value=Name)
thena_v1_pools = thena_v1_pools.drop('isStable', axis=1)

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/unchase/knightswap-bsc',
    verify=True,
    retries=3)

client = Client(
    transport=transport)

# First 1000 pool
query1 = gql('''
query {
  pairs(first: 1000, orderBy: syncAtTimestamp, orderDirection: desc) {
    id
    token0 {
      id
      symbol
      decimals
    }
    token1 {
      id
      symbol
      decimals
    }
  }
}
''')

response1 = client.execute(query1)

knightswap_pool1 = pd.json_normalize(response1['pairs'], max_level=1)

# Combination
knightswap_pools = pd.concat([knightswap_pool1], ignore_index=True)
knightswap_pools.columns = ['pool_address',
                            'token1', 'token1_symbol', 'token1_decimals',
                            'token2', 'token2_symbol', 'token2_decimals']
Name = pd.Series(["knightswap" for x in range(len(knightswap_pools.index))])
knightswap_pools.insert(loc=0, column='Name', value=Name)
knightswap_pools.insert(loc=2, column='fee', value=0.2)

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/unchase/nomiswap-bsc',
    verify=True,
    retries=3)

client = Client(
    transport=transport)

# First 1000 pool
query1 = gql('''
query {
  pairs(first: 1000, orderBy: syncAtTimestamp, orderDirection: desc) {
    id
    token0 {
      id
      symbol
      decimals
    }
    token1 {
      id
      symbol
      decimals
    }
  }
}
''')

response1 = client.execute(query1)

nomiswap_pool1 = pd.json_normalize(response1['pairs'], max_level=1)

# Combination
nomiswap_pools = pd.concat([nomiswap_pool1], ignore_index=True)
nomiswap_pools.columns = ['pool_address',
                          'token1', 'token1_symbol', 'token1_decimals',
                          'token2', 'token2_symbol', 'token2_decimals']
Name = pd.Series(["nomiswap" for x in range(len(nomiswap_pools.index))])
nomiswap_pools.insert(loc=0, column='Name', value=Name)
nomiswap_pools.insert(loc=2, column='fee', value=0.1)

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/unchase/babydoge-bsc',
    verify=True,
    retries=3)

client = Client(
    transport=transport)

# First 1000 pool
query1 = gql('''
query MyQuery {
  pairs {
    id
    token0 {
      id
      symbol
      decimals
    }
    token1 {
      id
      symbol
      decimals
    }
  }
}
''')

response1 = client.execute(query1)

babydoge_pool1 = pd.json_normalize(response1['pairs'], max_level=1)

# Combination
babydoge_pools = pd.concat([babydoge_pool1], ignore_index=True)
babydoge_pools.columns = ['pool_address',
                          'token1', 'token1_symbol', 'token1_decimals',
                          'token2', 'token2_symbol', 'token2_decimals']
Name = pd.Series(["babydoge" for x in range(len(babydoge_pools.index))])
babydoge_pools.insert(loc=0, column='Name', value=Name)
babydoge_pools.insert(loc=2, column='fee', value=0.3)

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/unchase/alitaswap-bsc',
    verify=True,
    retries=3)

client = Client(
    transport=transport)

# First 1000 pool
query1 = gql('''
query {
  pairs(first: 1000, orderBy: syncAtTimestamp, orderDirection: desc) {
    id
    token0 {
      id
      symbol
      decimals
    }
    token1 {
      id
      symbol
      decimals
    }
  }
}
''')

response1 = client.execute(query1)

alitaswap_pool1 = pd.json_normalize(response1['pairs'], max_level=1)

# Combination
alitaswap_pools = pd.concat([alitaswap_pool1], ignore_index=True)
alitaswap_pools.columns = ['pool_address',
                           'token1', 'token1_symbol', 'token1_decimals',
                           'token2', 'token2_symbol', 'token2_decimals']
Name = pd.Series(["alitaswap" for x in range(len(alitaswap_pools.index))])
alitaswap_pools.insert(loc=0, column='Name', value=Name)
alitaswap_pools.insert(loc=2, column='fee', value=0.1)

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/thinhpn/appleswap-bsc-chain',
    verify=True,
    retries=3)

client = Client(
    transport=transport)

# First 1000 pool
query1 = gql('''
query {
  pairs(first: 1000, orderBy: createdAtTimestamp, orderDirection: desc) {
    id
    token0 {
      id
      symbol
      decimals
    }
    token1 {
      id
      symbol
      decimals
    }
  }
}
''')

response1 = client.execute(query1)

appleswap_pool1 = pd.json_normalize(response1['pairs'], max_level=1)

# Combination
appleswap_pools = pd.concat([appleswap_pool1], ignore_index=True)
appleswap_pools.columns = ['pool_address',
                           'token1', 'token1_symbol', 'token1_decimals',
                           'token2', 'token2_symbol', 'token2_decimals']
Name = pd.Series(["appleswap" for x in range(len(appleswap_pools.index))])
appleswap_pools.insert(loc=0, column='Name', value=Name)
appleswap_pools.insert(loc=2, column='fee', value=0.1)

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/unchase/cafeswap-bsc',
    verify=True,
    retries=3)

client = Client(
    transport=transport)

# First 1000 pool
query1 = gql('''
query {
  pairs(first: 1000, orderBy: createdAtTimestamp, orderDirection: desc) {
    id
    token0 {
      id
      symbol
      decimals
    }
    token1 {
      id
      symbol
      decimals
    }
  }
}
''')

response1 = client.execute(query1)

cafeswap_pool1 = pd.json_normalize(response1['pairs'], max_level=1)

# Combination
cafeswap_pools = pd.concat([cafeswap_pool1], ignore_index=True)
cafeswap_pools.columns = ['pool_address',
                          'token1', 'token1_symbol', 'token1_decimals',
                          'token2', 'token2_symbol', 'token2_decimals']
Name = pd.Series(["cafeswap" for x in range(len(cafeswap_pools.index))])
cafeswap_pools.insert(loc=0, column='Name', value=Name)
cafeswap_pools.insert(loc=2, column='fee', value=0.1)

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/unchase/cheeseswap-bsc',
    verify=True,
    retries=3)

client = Client(
    transport=transport)

# First 1000 pool
query1 = gql('''
query {
  pairs(first: 1000, orderBy: createdAtTimestamp, orderDirection: desc) {
    id
    token0 {
      id
      symbol
      decimals
    }
    token1 {
      id
      symbol
      decimals
    }
  }
}
''')

response1 = client.execute(query1)

cheeseswap_pool1 = pd.json_normalize(response1['pairs'], max_level=1)

# Combination
cheeseswap_pools = pd.concat([cheeseswap_pool1], ignore_index=True)
cheeseswap_pools.columns = ['pool_address',
                            'token1', 'token1_symbol', 'token1_decimals',
                            'token2', 'token2_symbol', 'token2_decimals']
Name = pd.Series(["cheeseswap" for x in range(len(cheeseswap_pools.index))])
cheeseswap_pools.insert(loc=0, column='Name', value=Name)
cheeseswap_pools.insert(loc=2, column='fee', value=0.1)

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/unchase/coinswap-bsc',
    verify=True,
    retries=3)

client = Client(
    transport=transport)

# First 1000 pool
query1 = gql('''
query {
  pairs(first: 1000, orderBy: createdAtTimestamp, orderDirection: desc) {
    id
    token0 {
      id
      symbol
      decimals
    }
    token1 {
      id
      symbol
      decimals
    }
  }
}
''')

response1 = client.execute(query1)

coinswap_pool1 = pd.json_normalize(response1['pairs'], max_level=1)
coinswap_pool1

# Combination
coinswap_pools = pd.concat([coinswap_pool1], ignore_index=True)
coinswap_pools.columns = ['pool_address',
                          'token1', 'token1_symbol', 'token1_decimals',
                          'token2', 'token2_symbol', 'token2_decimals']
Name = pd.Series(["coinswap" for x in range(len(coinswap_pools.index))])
coinswap_pools.insert(loc=0, column='Name', value=Name)
coinswap_pools.insert(loc=2, column='fee', value=0.1)

all_pool = pd.concat(
    [apeswap_pools, biswap_pools, ellipsis_pools, pancakeswap_pools, sushiswap_pools, sushiswapv3_pools,
     uniswap_pools, thena_fusion_pools, thena_v1_pools, knightswap_pools, nomiswap_pools, babydoge_pools,
     alitaswap_pools, appleswap_pools, cafeswap_pools, cheeseswap_pools, coinswap_pools], ignore_index=True)
all_pool['tvl'] = all_pool['tvl'].fillna(0)
all_pool['fee'] = all_pool['fee'].apply(float)
all_pool['fee'] = all_pool['fee'].div(100)
pool_data = all_pool.copy()
bsc_pool = all_pool.copy()
bsc_pool['tvl'] = bsc_pool['tvl'].apply(float)
# bsc_pool = bsc_pool[bsc_pool['tvl'] > 100000].reset_index()
bsc_pool = bsc_pool.sort_values(by=['tvl'], ascending=False)
route_pool = bsc_pool.head(150)
final_pool = route_pool[['Name', 'pool_address', 'fee', 'token1', 'token2']]
# pool_id = final_pool.index + 1
# final_pool.insert(loc=0, column='pool_id', value=pool_id)
pool_used = final_pool.copy()

pool_used_data = pool_data.to_json(orient='records')
pool_used_name = f"pool_{timestamp}.json"
# pool_used_name =  "pool.json"
pool_used_path = os.path.join(pool_directory, pool_used_name)
with open(pool_used_path, "w") as outfile:
    outfile.write(pool_used_data)

token1 = all_pool[['token1_symbol', 'token1', 'token1_decimals']].drop_duplicates()
token1.columns = [['token_symbol', 'token_address', 'token_decimals']]
token2 = all_pool[['token2_symbol', 'token2', 'token2_decimals']].drop_duplicates()
token2.columns = [['token_symbol', 'token_address', 'token_decimals']]

token = pd.concat([token1, token2], ignore_index=True)
token = token.drop_duplicates()
token.columns = ['symbol', 'token', 'decimals']
token = token.drop_duplicates(subset="token")
token = token.reset_index(drop=True)
# token_id = token.index + 1
# token.insert(loc=0, column='token_id', value=token_id)

token_data = token.to_json(orient='records')
token_name = f"token_{timestamp}.json"
# token_name = "token.json"
token_path = os.path.join(token_directory, token_name)
with open(token_path, "w") as outfile:
    outfile.write(token_data)

df_slice = pool_used.iloc[:, 3:]

# flatten the DataFrame to a 1D array, ignore NaN values, keep unique values, and convert to a list
unique_values = pd.unique(df_slice.values.ravel('K'))
clean_values = [value for value in unique_values if not pd.isnull(value)]
token_inside = clean_values.copy()

# We find all possible connections between two tokens based on the contract information
import itertools
from collections import defaultdict

df = pool_used.copy()
df = df.fillna(0)
for column in df.columns:
    if 'token' in column:
        df[column] = df[column]


def df_to_dict(df):
    graph = defaultdict(list)

    for _, row in df.iterrows():
        contract_id = row['pool_address']
        tokens = row[3:].dropna().values
        for token in tokens:
            # Ignore placeholder values when creating the graph
            if token != 0:
                connected_tokens = [(t, contract_id) for t in tokens if t != token and t != 0]
                graph[token].extend(connected_tokens)

    return graph


graph = df_to_dict(df)

depth_limit = 2  # The depth limit determines the route distance
pair_combinations = list(itertools.combinations(token_inside, 2))


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
            for next, contract in graph[vertex]:
                if any(t == next for t, _ in path) or next == 0:
                    continue
                elif next == end:
                    yield path + [(next, contract)]
                else:
                    queue.append((path + [(next, contract)], contracts + [contract], depth + 1))


def format_paths(paths):
    formatted = []
    for path in paths:
        formatted_path = []
        for i in range(len(path) - 1):
            formatted_path.append([(path[i][0], path[i][1]), (path[-1][0], path[-1][1])])
        formatted.append(formatted_path)
    return formatted


results = []
for pair in pair_combinations:
    start, end = pair
    paths = bfs_paths(graph, start, end, depth_limit)
    formatted_paths = format_paths(paths)
    results.extend(formatted_paths)

output = pd.DataFrame(results)
output = output.fillna('[(None, None),(None, None)]')

one = output.iloc[:, 0]
two = output.iloc[:, 2]

column1 = []
column2 = []
column3 = []
column4 = []
column5 = []

# Initialize empty lists for each column
for row in one:
    # Convert the row value to a string and remove the square brackets
    row_str = str(row).strip('[]')

    # Split the row by comma and extract the values
    values = row_str.split(', ')
    column1.append(values[0].split('(')[1])
    column2.append(values[2].split('(')[1])
    column3.append(values[1].split(')')[0])
    column4.append(values[3].split(')')[0])

for row in two:
    # Convert the row value to a string and remove the square brackets
    line_str = str(row).strip('[]')

    # Split the row by comma and extract the values
    cells = line_str.split(', ')
    column5.append(cells[1].split(')')[0])

data_dict = {'token_1': column1, 'token_2': column2, 'contract_1': column3, 'contract_2': column4,
             'contract_3': column5}

# Create a new DataFrame from the dictionary
route = pd.DataFrame(data_dict)


# All the possible connections between two tokens have been found, and this dataframe will be stored into route table
def calculate_distance(row):
    if row['contract_1'] == 'None':
        return 1
    elif row['contract_3'] == 'None':
        return 2
    else:
        return 3


# Apply the function to each row in the dataframe to create the new 'distance' column
route['distance'] = route.apply(calculate_distance, axis=1)
# route['route_id'] = route.index + 1
# first_column = route.pop('route_id')
# route.insert(0, 'route_id', first_column)

route_data = route.to_json(orient='records', lines=True)
route_name = f"route_{timestamp}.json"
# route_name = "route.json"
route_path = os.path.join(route_directory, route_name)
with open(route_path, "w") as outfile:
    outfile.write(route_data)
