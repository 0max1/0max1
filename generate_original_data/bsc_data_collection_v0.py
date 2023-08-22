import pandas as pd
import plotly.express as px
import json
from convert_csv_to_json import *
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/messari/apeswap-bsc',
    verify=True,
    retries=3)

client = Client(
    transport = transport)

#First 1000 pool
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

df11 = pd.json_normalize(response1['liquidityPools'],max_level=1)
df12 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[0])
df13 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[1])
df14 = pd.json_normalize(df11['fees'].apply(pd.Series)[2])

apeswap_pool1 = pd.concat([df11[['id','totalValueLockedUSD']],df14,df12,df13],axis=1)
apeswap_pool1

#pool 1001-2000
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

df21 = pd.json_normalize(response2['liquidityPools'],max_level=1)
df22 = pd.json_normalize(df21['inputTokens'].apply(pd.Series)[0])
df23 = pd.json_normalize(df21['inputTokens'].apply(pd.Series)[1])
df24 = pd.json_normalize(df21['fees'].apply(pd.Series)[2])

apeswap_pool2 = pd.concat([df21[['id','totalValueLockedUSD']],df24,df22,df23],axis=1)
apeswap_pool2

#pool 2001-3000
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

df31 = pd.json_normalize(response3['liquidityPools'],max_level=1)
df32 = pd.json_normalize(df31['inputTokens'].apply(pd.Series)[0])
df33 = pd.json_normalize(df31['inputTokens'].apply(pd.Series)[1])
df34 = pd.json_normalize(df31['fees'].apply(pd.Series)[2])

apeswap_pool3 = pd.concat([df31[['id','totalValueLockedUSD']],df34,df32,df33],axis=1)
apeswap_pool3

#pool 3001-4000
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

df41 = pd.json_normalize(response4['liquidityPools'],max_level=1)
df42 = pd.json_normalize(df41['inputTokens'].apply(pd.Series)[0])
df43 = pd.json_normalize(df41['inputTokens'].apply(pd.Series)[1])
df44 = pd.json_normalize(df41['fees'].apply(pd.Series)[2])

apeswap_pool4 = pd.concat([df41[['id','totalValueLockedUSD']],df44,df42,df43],axis=1)
apeswap_pool4

#pool 4001-5000
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

df51 = pd.json_normalize(response5['liquidityPools'],max_level=1)
df52 = pd.json_normalize(df51['inputTokens'].apply(pd.Series)[0])
df53 = pd.json_normalize(df51['inputTokens'].apply(pd.Series)[1])
df54 = pd.json_normalize(df51['fees'].apply(pd.Series)[2])

apeswap_pool5 = pd.concat([df51[['id','totalValueLockedUSD']],df54,df52,df53],axis=1)
apeswap_pool5

#Combination
apeswap_pools = pd.concat([apeswap_pool1,apeswap_pool2,apeswap_pool3,apeswap_pool4,apeswap_pool5], ignore_index=True)
apeswap_pools.columns = ['pool_address','tvl','fee',
                         'token1','token1_symbol','token1_decimals',
                         'token2','token2_symbol','token2_decimals']
Name = pd.Series(["apeswap" for x in range(len(apeswap_pools.index))])
apeswap_pools.insert(loc=0, column='Name', value=Name)
apeswap_pools

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/messari/biswap-bsc',
    verify=True,
    retries=3)

client = Client(
    transport = transport)

#First 1000 pool
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

df11 = pd.json_normalize(response1['liquidityPools'],max_level=1)
df12 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[0])
df13 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[1])
df14 = pd.json_normalize(df11['fees'].apply(pd.Series)[2])

biswap_pool1 = pd.concat([df11[['id','totalValueLockedUSD']],df14,df12,df13],axis=1)
biswap_pool1

#pool 1001-2000
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

df21 = pd.json_normalize(response2['liquidityPools'],max_level=1)
df22 = pd.json_normalize(df21['inputTokens'].apply(pd.Series)[0])
df23 = pd.json_normalize(df21['inputTokens'].apply(pd.Series)[1])
df24 = pd.json_normalize(df21['fees'].apply(pd.Series)[2])

biswap_pool2 = pd.concat([df21[['id','totalValueLockedUSD']],df24,df22,df23],axis=1)
biswap_pool2

#pool 2001-3000
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

df31 = pd.json_normalize(response3['liquidityPools'],max_level=1)
df32 = pd.json_normalize(df31['inputTokens'].apply(pd.Series)[0])
df33 = pd.json_normalize(df31['inputTokens'].apply(pd.Series)[1])
df34 = pd.json_normalize(df31['fees'].apply(pd.Series)[2])

biswap_pool3 = pd.concat([df31[['id','totalValueLockedUSD']],df34,df32,df33],axis=1)
biswap_pool3

#Combination
biswap_pools = pd.concat([biswap_pool1,biswap_pool2,biswap_pool3], ignore_index=True)
biswap_pools.columns = ['pool_address','tvl','fee',
                         'token1','token1_symbol','token1_decimals',
                         'token2','token2_symbol','token2_decimals']
Name = pd.Series(["biswap" for x in range(len(biswap_pools.index))])
biswap_pools.insert(loc=0, column='Name', value=Name)
biswap_pools

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/messari/ellipsis-finance-bsc',
    verify=True,
    retries=3)

client = Client(
    transport = transport)

#First 1000 pool
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

df11 = pd.json_normalize(response1['liquidityPools'],max_level=1)
df12 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[0])
df13 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[1])
df14 = pd.json_normalize(df11['fees'].apply(pd.Series)[2])

ellipsis_pool1 = pd.concat([df11[['id','totalValueLockedUSD']],df14,df12,df13],axis=1)
ellipsis_pool1

#Combination
ellipsis_pools = pd.concat([ellipsis_pool1], ignore_index=True)
ellipsis_pools.columns = ['pool_address','tvl','fee',
                         'token1','token1_symbol','token1_decimals',
                         'token2','token2_symbol','token2_decimals']
Name = pd.Series(["ellipsis" for x in range(len(biswap_pools.index))])
ellipsis_pools.insert(loc=0, column='Name', value=Name)
ellipsis_pools

transport = RequestsHTTPTransport(
    url='https://open-platform.nodereal.io/8c4aebac194d4b5790f49e1df0d83c2f/pancakeswap-free/graphql',
    verify=True,
    retries=3)

client = Client(
    transport = transport)

#First 1000 pool
query1 = gql('''
query {
   pairs(first: 1000, skip: 0, orderBy: trackedReserveBNB, orderDirection: desc)
    {
      id
      reserveUSD
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
pancakeswapv2_pool1 = pd.json_normalize(response1['pairs'],max_level=1)
pancakeswapv2_pool1

#pool 1001-2000
query2 = gql('''
query {
   pairs(first: 1000, skip: 1000, orderBy: trackedReserveBNB, orderDirection: desc)
    {
      id
      reserveUSD
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

response2 = client.execute(query2)
pancakeswapv2_pool2 = pd.json_normalize(response2['pairs'],max_level=1)
pancakeswapv2_pool2

#pool 2001-3000
query3 = gql('''
query {
   pairs(first: 1000, skip: 2000, orderBy: trackedReserveBNB, orderDirection: desc)
    {
      id
      reserveUSD
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

response3 = client.execute(query3)
pancakeswapv2_pool3 = pd.json_normalize(response3['pairs'],max_level=1)
pancakeswapv2_pool3

# Combination
pancakeswapv2_pools = pd.concat([pancakeswapv2_pool1,pancakeswapv2_pool2,pancakeswapv2_pool3], ignore_index=True)
pancakeswapv2_pools.columns = ['pool_address','tvl',
                               'token1','token1_symbol','token1_decimals',
                               'token2','token2_symbol','token2_decimals']
Name = pd.Series(["pancakeswap_v2" for x in range(len(pancakeswapv2_pools.index))])
pancakeswapv2_pools.insert(loc=0, column='Name', value=Name)
pancakeswapv2_pools.insert(loc=3, column='fee', value=0.25)
pancakeswapv2_pools

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/messari/pancakeswap-v3-bsc',
    verify=True,
    retries=3)

client = Client(
    transport = transport)

#First 1000 pool
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

df11 = pd.json_normalize(response1['liquidityPools'],max_level=1)
df12 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[0])
df13 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[1])
df14 = pd.json_normalize(df11['fees'].apply(pd.Series)[2])

pancakeswap_pool1 = pd.concat([df11[['id','totalValueLockedUSD']],df14,df12,df13],axis=1)
pancakeswap_pool1

#pool 1001-2000
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

df21 = pd.json_normalize(response2['liquidityPools'],max_level=1)
df22 = pd.json_normalize(df21['inputTokens'].apply(pd.Series)[0])
df23 = pd.json_normalize(df21['inputTokens'].apply(pd.Series)[1])
df24 = pd.json_normalize(df21['fees'].apply(pd.Series)[2])

pancakeswap_pool2 = pd.concat([df21[['id','totalValueLockedUSD']],df24,df22,df23],axis=1)
pancakeswap_pool2

#pool 2001-3000
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

df31 = pd.json_normalize(response3['liquidityPools'],max_level=1)
df32 = pd.json_normalize(df31['inputTokens'].apply(pd.Series)[0])
df33 = pd.json_normalize(df31['inputTokens'].apply(pd.Series)[1])
df34 = pd.json_normalize(df31['fees'].apply(pd.Series)[2])

pancakeswap_pool3 = pd.concat([df31[['id','totalValueLockedUSD']],df34,df32,df33],axis=1)
pancakeswap_pool3

#pool 3001-4000
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

df41 = pd.json_normalize(response4['liquidityPools'],max_level=1)
df42 = pd.json_normalize(df41['inputTokens'].apply(pd.Series)[0])
df43 = pd.json_normalize(df41['inputTokens'].apply(pd.Series)[1])
df44 = pd.json_normalize(df41['fees'].apply(pd.Series)[2])

pancakeswap_pool4 = pd.concat([df41[['id','totalValueLockedUSD']],df44,df42,df43],axis=1)
pancakeswap_pool4

#pool 4001-5000
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

df51 = pd.json_normalize(response5['liquidityPools'],max_level=1)
df52 = pd.json_normalize(df51['inputTokens'].apply(pd.Series)[0])
df53 = pd.json_normalize(df51['inputTokens'].apply(pd.Series)[1])
df54 = pd.json_normalize(df51['fees'].apply(pd.Series)[2])

pancakeswap_pool5 = pd.concat([df51[['id','totalValueLockedUSD']],df54,df52,df53],axis=1)
pancakeswap_pool5

#pool 5001-6000
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

df61 = pd.json_normalize(response6['liquidityPools'],max_level=1)
df62 = pd.json_normalize(df61['inputTokens'].apply(pd.Series)[0])
df63 = pd.json_normalize(df61['inputTokens'].apply(pd.Series)[1])
df64 = pd.json_normalize(df61['fees'].apply(pd.Series)[2])

pancakeswap_pool6 = pd.concat([df61[['id','totalValueLockedUSD']],df64,df62,df63],axis=1)
pancakeswap_pool6

#Combination
pancakeswap_pools = pd.concat([pancakeswap_pool1,pancakeswap_pool2,pancakeswap_pool3,pancakeswap_pool4,pancakeswap_pool5,pancakeswap_pool6], ignore_index=True)
pancakeswap_pools.columns = ['pool_address','tvl','fee',
                         'token1','token1_symbol','token1_decimals',
                         'token2','token2_symbol','token2_decimals']
Name = pd.Series(["pancakeswap" for x in range(len(pancakeswap_pools.index))])
pancakeswap_pools.insert(loc=0, column='Name', value=Name)
pancakeswap_pools

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/messari/sushiswap-bsc',
    verify=True,
    retries=3)

client = Client(
    transport = transport)

#First 1000 pool
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

df11 = pd.json_normalize(response1['liquidityPools'],max_level=1)
df12 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[0])
df13 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[1])
df14 = pd.json_normalize(df11['fees'].apply(pd.Series)[2])

sushiswap_pool1 = pd.concat([df11[['id','totalValueLockedUSD']],df14,df12,df13],axis=1)
sushiswap_pool1

#pool 1001-2000
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

df21 = pd.json_normalize(response2['liquidityPools'],max_level=1)
df22 = pd.json_normalize(df21['inputTokens'].apply(pd.Series)[0])
df23 = pd.json_normalize(df21['inputTokens'].apply(pd.Series)[1])
df24 = pd.json_normalize(df21['fees'].apply(pd.Series)[2])

sushiswap_pool2 = pd.concat([df21[['id','totalValueLockedUSD']],df24,df22,df23],axis=1)
sushiswap_pool2

#Combination
sushiswap_pools = pd.concat([sushiswap_pool1,sushiswap_pool2], ignore_index=True)
sushiswap_pools.columns = ['pool_address','tvl','fee',
                         'token1','token1_symbol','token1_decimals',
                         'token2','token2_symbol','token2_decimals']
Name = pd.Series(["sushiswap" for x in range(len(sushiswap_pools.index))])
sushiswap_pools.insert(loc=0, column='Name', value=Name)
sushiswap_pools

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/messari/sushiswap-v3-bsc',
    verify=True,
    retries=3)

client = Client(
    transport = transport)

#First 1000 pool
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

df11 = pd.json_normalize(response1['liquidityPools'],max_level=1)
df12 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[0])
df13 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[1])
df14 = pd.json_normalize(df11['fees'].apply(pd.Series)[2])

sushiswapv3_pool1 = pd.concat([df11[['id','totalValueLockedUSD']],df14,df12,df13],axis=1)
sushiswapv3_pool1

#Combination
sushiswapv3_pools = pd.concat([sushiswapv3_pool1], ignore_index=True)
sushiswapv3_pools.columns = ['pool_address','tvl','fee',
                         'token1','token1_symbol','token1_decimals',
                         'token2','token2_symbol','token2_decimals']
Name = pd.Series(["sushiswapv3" for x in range(len(sushiswapv3_pools.index))])
sushiswapv3_pools.insert(loc=0, column='Name', value=Name)
sushiswapv3_pools

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/messari/uniswap-v3-bsc',
    verify=True,
    retries=3)

client = Client(
    transport = transport)

#First 1000 pool
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

df11 = pd.json_normalize(response1['liquidityPools'],max_level=1)
df12 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[0])
df13 = pd.json_normalize(df11['inputTokens'].apply(pd.Series)[1])
df14 = pd.json_normalize(df11['fees'].apply(pd.Series)[2])

uniswap_pool1 = pd.concat([df11[['id','totalValueLockedUSD']],df14,df12,df13],axis=1)
uniswap_pool1

#pool 1001-2000
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

df21 = pd.json_normalize(response2['liquidityPools'],max_level=1)
df22 = pd.json_normalize(df21['inputTokens'].apply(pd.Series)[0])
df23 = pd.json_normalize(df21['inputTokens'].apply(pd.Series)[1])
df24 = pd.json_normalize(df21['fees'].apply(pd.Series)[2])

uniswap_pool2 = pd.concat([df21[['id','totalValueLockedUSD']],df24,df22,df23],axis=1)
uniswap_pool2

#Combination
uniswap_pools = pd.concat([uniswap_pool1,uniswap_pool2], ignore_index=True)
uniswap_pools.columns = ['pool_address','tvl','fee',
                         'token1','token1_symbol','token1_decimals',
                         'token2','token2_symbol','token2_decimals']
Name = pd.Series(["uniswap" for x in range(len(uniswap_pools.index))])
uniswap_pools.insert(loc=0, column='Name', value=Name)
uniswap_pools

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/thenaursa/thena-fusion',
    verify=True,
    retries=3)

client = Client(
    transport = transport)

#First 1000 pool
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

thena_fusion_pool1 = pd.json_normalize(response1['pools'],max_level=1)
thena_fusion_pool1

#Combination
thena_fusion_pools = pd.concat([thena_fusion_pool1], ignore_index=True)
thena_fusion_pools.columns = ['pool_address','tvl','fee',
                         'token1','token1_symbol','token1_decimals',
                         'token2','token2_symbol','token2_decimals']
Name = pd.Series(["thena_fusion" for x in range(len(thena_fusion_pools.index))])
thena_fusion_pools.insert(loc=0, column='Name', value=Name)
thena_fusion_pools['fee'] = thena_fusion_pools['fee'].apply(int)/10000
thena_fusion_pools

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/thenaursa/thena-v1',
    verify=True,
    retries=3)

client = Client(
    transport = transport)

#First 1000 pool
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

thena_v1_pool1 = pd.json_normalize(response1['pairs'],max_level=1)
thena_v1_pool1

#Combination
thena_v1_pools = pd.concat([thena_v1_pool1], ignore_index=True)
thena_v1_pools.columns = ['pool_address','tvl', 'isStable',
                         'token1','token1_symbol','token1_decimals',
                         'token2','token2_symbol','token2_decimals']
Name = pd.Series(["thena_v1" for x in range(len(thena_v1_pools.index))])
thena_v1_pools.insert(loc=0, column='Name', value=Name)
# thena_v1_pools['isStable'] = thena_fusion_pools['fee'].apply(int)/10000
thena_v1_pools['fee'] = thena_v1_pools['isStable'].apply(lambda x: 0.01 if x == True else 0.2)
thena_v1_pools = thena_v1_pools[['Name','pool_address','tvl', 'fee',
                         'token1','token1_symbol','token1_decimals',
                         'token2','token2_symbol','token2_decimals']]
thena_v1_pools

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/unchase/knightswap-bsc',
    verify=True,
    retries=3)

client = Client(
    transport = transport)

#First 1000 pool
query1 = gql('''
query {
  pairs(first: 1000, orderBy: syncAtTimestamp, orderDirection: desc) {
    id
    syncAtTimestamp
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

knightswap_pool1 = pd.json_normalize(response1['pairs'],max_level=1)
knightswap_pool1

#Combination
knightswap_pools = pd.concat([knightswap_pool1], ignore_index=True)
knightswap_pools.columns = ['pool_address','syncAtTimestamp',
                         'token1','token1_symbol','token1_decimals',
                         'token2','token2_symbol','token2_decimals']
Name = pd.Series(["knightswap" for x in range(len(knightswap_pools.index))])
knightswap_pools.insert(loc=0, column='Name', value=Name)
knightswap_pools.insert(loc=3, column='fee', value=0.2)
knightswap_pools

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/unchase/nomiswap-bsc',
    verify=True,
    retries=3)

client = Client(
    transport = transport)

#First 1000 pool
query1 = gql('''
query {
  pairs(first: 1000, orderBy: syncAtTimestamp, orderDirection: desc) {
    id
    syncAtTimestamp
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

nomiswap_pool1 = pd.json_normalize(response1['pairs'],max_level=1)
nomiswap_pool1

# Combination
nomiswap_pools = pd.concat([nomiswap_pool1], ignore_index=True)
nomiswap_pools.columns = ['pool_address','syncAtTimestamp',
                         'token1','token1_symbol','token1_decimals',
                         'token2','token2_symbol','token2_decimals']
Name = pd.Series(["nomiswap" for x in range(len(nomiswap_pools.index))])
nomiswap_pools.insert(loc=0, column='Name', value=Name)
nomiswap_pools.insert(loc=3, column='fee', value=0.1)
nomiswap_pools

transport = RequestsHTTPTransport(
    url='https://api.thegraph.com/subgraphs/name/unchase/babydoge-bsc',
    verify=True,
    retries=3)

client = Client(
    transport = transport)

#First 1000 pool
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

babydoge_pool1 = pd.json_normalize(response1['pairs'],max_level=1)


#Combination
babydoge_pools = pd.concat([babydoge_pool1], ignore_index=True)
babydoge_pools.columns = ['pool_address',
                         'token1','token1_symbol','token1_decimals',
                         'token2','token2_symbol','token2_decimals']
Name = pd.Series(["babydoge" for x in range(len(babydoge_pools.index))])
babydoge_pools.insert(loc=0, column='Name', value=Name)
babydoge_pools.insert(loc=2, column='fee', value=0.3)


bsc_pool= pd.concat([apeswap_pools,biswap_pools, ellipsis_pools, pancakeswapv2_pools, pancakeswap_pools, sushiswap_pools, sushiswapv3_pools, uniswap_pools, thena_fusion_pools, thena_v1_pools], ignore_index=True)


bsc_pool.to_csv('bsc_pool.csv', encoding='utf-8-sig', escapechar=',')
