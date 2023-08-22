```
MevMax_Project/
│
├── main.py
│
├── main_test.py
│
├── main/
│   ├── __init__.py
│   ├── get_row_data.py
│   ├── init_db.py
│   ├── update_db.py
│   ├── get_route_data.py
│   ├── no_routes_pairs.txt
│   └── routes_data
│
├── generate_original_data/
│   ├── __init__.py
│   ├── bsc_data_collection_v0.py
│   ├── bsc_data_collection_v1.py
│   ├── bsc_data_collection_v1_1.py
│   ├── bsc_data_collection_v2.py
│   ├── convert_csv_to_json.py
│   └── original_data
│   		├── bsc_pool.json
│				└── bsc_token.json
│	
├── init_mevmax_db/
│   ├── __init__.py
│   ├── create_mevmax_db.py
│   ├── init_blockchain_data.py
│   ├── init_token_data.py
│   ├── init_pair_data.py
│   └── init_pool_protocol_poolpair_data.py
│		
├── update_mevmax_db/
│   ├── __init__.py
│   ├── update_blockchain_data.py
│   ├── update_token_pair_data.py
│   └── update_pool_protocol_poolpair_data
│ 
├── cal_route_data/
│   ├── __init__.py
│   ├── generate_pairs_pools_data.py
│   ├── cal_route_pairs_data_encapsulated_v0.py
│   ├── cal_route_pairs_data_encapsulated_standardization_v1.py
│   ├── cal_route_pairs_data_multithreading_v2_0.py
│   ├── cal_route_pairs_data_multithreading_fix_bugs_v2_1.py
│   ├── cal_route_pairs_data_networkx_v3.py
│   ├── generate_pairs_pools_data.py
│   ├── update_null_route_pair_flag.py
│   └── pairs_pool_data
│   		├── pair_data.csv
│				└── pool_data.csv
│
├── statistical_analysis_data/
│   ├── analyze_pool_data_tvl.py
│   ├── convert_tokens.py
│   ├── json_token_comparison.py
│   └── analyzed_data
│   		├── tvl_stats.csv
│				└── ......
│
├── config/
│   └── mevmax_config.ini
│ 
├── requirements.txt
│
└── README.md
```



# 1. main

```
├── main/
│   ├── __init__.py
│   ├── get_row_data.py
│   ├── init_db.py
│   ├── update_db.py
│   ├── get_route_data.py
│   ├── no_routes_pairs.txt
│   └── routes_data
```



# 2. generate_original_data package

```
├── generate_original_data/
│   ├── __init__.py
│   ├── bsc_data_collection_v0.py
│   ├── bsc_data_collection_v1.py
│   ├── bsc_data_collection_v1_1.py
│   ├── bsc_data_collection_v2.py
│   ├── convert_csv_to_json.py
│   └── original_data
│   		├── bsc_pool.json
│				└── bsc_token.json
```

2.1 bsc_data_collection_v0.py

This file collects liquidity pool data from the Apeswap BSC subgraph API. Key capabilities:

- Imports core modules like pandas, plotly, json, and gql for data analysis and API access
- Establishes a client connection to the Apeswap BSC subgraph API endpoint
- Executes a GraphQL query to fetch data on the top 1000 liquidity pools, focusing on fields like id, totalValueLockedUSD, fees, and inputTokens
- Leverages pandas to process and standardize the acquired data

Overall, this script provides a starting point for gathering and normalizing liquidity pool metrics from the Apeswap BSC subgraph.

2.2 bsc_data_collection_v1.py

This is an updated version of the liquidity pool data collection script. Key enhancements:

- Adds metadata like author, date, and version
- Imports core modules like pandas, gql, numpy, and datetime
- Defines timestamp and directory path variables for better structure
- Retains the GraphQL query to fetch data on the top 1000 pools
- Represent an incremental improvement over v0.py

2.3 bsc_data_collection_v1_1.py

This makes minor modifications to add path checking and ensure target directories exist:

- Imports modules like gql, datetime, os, and convert_csv_to_json
- Checks/creates the token_directory and pool_directory paths
- Overall small change to enable path validation before data storage

2.4 bsc_data_collection_v2.py

This represents a more modular, production-ready version:

- Imports robust set of modules: os, datetime, gql, tqdm, pandas
- Defines script directory and path fetching function
- Adds a core fetch_and_process_liquidity_pools function to get data in batches
- Enables more scalable and maintainable data collection process

2.5 convert_csv_to_json.py

This provides a utility function to convert liquidity pool CSV data to JSON:

- Defines script directory path
- Implements pool_convert_csv_to_json method to read CSV into DataFrame and convert rows to JSON
- Enables easy transformation of data format after collection



# 3. init_mevmax_db package

```
├── init_mevmax_db/
│   ├── __init__.py
│   ├── create_mevmax_db.py
│   ├── init_blockchain_data.py
│   ├── init_token_data.py
│   ├── init_pair_data.py
│   └── init_pool_protocol_poolpair_data.py
```



# 4. update_mevmax_db package

```
├── update_mevmax_db/
│   ├── __init__.py
│   ├── update_blockchain_data.py
│   ├── update_token_pair_data.py
│   └── update_pool_protocol_poolpair_data
```



# 5. cal_route_data package

```
├── cal_route_data/
│   ├── __init__.py
│   ├── generate_pairs_pools_data.py
│   ├── cal_route_pairs_data_encapsulated_v0.py
│   ├── cal_route_pairs_data_encapsulated_standardization_v1.py
│   ├── cal_route_pairs_data_multithreading_v2_0.py
│   ├── cal_route_pairs_data_multithreading_fix_bugs_v2_1.py
│   ├── cal_route_pairs_data_networkx_v3.py
│   ├── generate_pairs_pools_data.py
│   ├── update_null_route_pair_flag.py
│   └── pairs_pool_data
│   		├── pair_data.csv
│				└── pool_data.csv
```

5.1 cal_route_pairs_data_encapsulated_v0.py

- The original code has been encapsulated.

5.2 cal_route_pairs_data_encapsulated_standardization_v1.py

- The output format has been standardized.

5.3 cal_route_pairs_data_multithreading_v2.0.py

- Multithreading has been introduced to improve performance.

5.4 cal_route_pairs_data_multithreading_fix_bugs_v2.1.py

- Removed null values, logged empty pairs_id, and recorded both total_pairs_find_routes and total_pairs_with_null_routes.

5.5 cal_route_pairs_data_networkx_v3.py

- Description: Utilized the networkx package to generate a graph for route retrieval.

5.6 generate_pairs_pools_data.py

* Description: Search the database for data, generate source data for cal_route_pairs.py to run..



# 6. statistical_analysis_data package

```
├── statistical_analysis_data/
│   ├── analyze_pool_data_tvl.py
│   ├── convert_tokens.py
│   ├── json_token_comparison.py
│   └── analyzed_data
│   		├── tvl_stats.csv
│				└── ......
```

Code for data analysis and statistics on the generated data and intermediate data, and miscellaneous code.



# 7. config

```
├── config/
│   └── mevmax_config.ini
```

7.1 **[GET_ROUTE_DATA] Section**:

- `pool_file`: Specifies the path to the CSV file containing pool data.
- `pair_file`: Indicates the path to the CSV file that holds pair data.
- `num_holders`: Sets a threshold for the number of token holders.
- `pairs_limit`: Limits the number of pairs to be processed. A value of `-1` means no limit.
- `min_tvl`: Sets the minimum Total Value Locked (TVL) for a pool to be considered.
- `depth_limit`: Determines the depth to which the route data should be fetched.
- `num_processes`: Specifies the number of processes to be used.

7.2 **[UPDATE_DB] & [INIT_DB] Sections**:

- Both sections have similar configurations.
- `token_data_path`: Path to the JSON file containing token data.
- `pool_data_path`: Path to the JSON file containing pool data.
- `blockchain_name`: Specifies the name of the blockchain, which in this case is "Polygon".

7.3 **[MAIN] Section**:

- `op_type`: Determines the operation type. It can either be `init` (for initialization) or `update`.

7.4 **[DATABASE] Section**:

- Contains database connection details:
  - `user`: The username for the database.
  - `password`: The password associated with the user.
  - `host`: The host address of the database server.
  - `database`: The name of the database.

