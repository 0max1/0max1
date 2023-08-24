<h1 style="text-align:center">MevMax Project </h1>

<h6 style="text-align:right">Author: Justin Jia </h1>
<h6 style="text-align:right">Edited: AUG 23, 2023</h1>
<h6 style="text-align:right">Version: 1.5 </h1>


This guide outlines the construction of the Definer MevMax database, utilizing RDS Postgres DB and Amazon Neptune or Network Algorithm. The RDS Postgres DB manages data related to DeFi liquidity pools, tokens, trading pairs, protocols, and routes. It enables data management, aggregation, and interaction with the "Arbitrage Bot."


The database design includes tables for pools, protocols, blockchains, pairs, and tokens, facilitating efficient data retrieval through APIs.

The database design ensures efficient data management and seamless interactions for the Definer MevMax.

##  1. Overview

This guide provides instructions for building the database of the Definer MevMax. The primary database technology utilized is **RDS Postgres DB**, which serves as a relational database to efficiently manage data related to DeFi liquidity pools, liquidity tokens, trading pairs, protocols, and routes.

**Functionality achieved using RDS Postgres DB:**

1. **Data Management:** The RDS Postgres DB efficiently records, deletes, modifies, and queries data pertaining to DeFi liquidity pools, liquidity tokens, trading pairs, protocols, and routes.
2. **Aggregation:** RDS Postgres DB is employed to calculate the total count of tokens, pools, and pairs for each blockchain. Additionally, the database can compute the total value locked (TVL) and monitor the coverage of swaps.
3. **Interact with "Arbitrage Bot"**: The Definer MevMax requires a database solution that can efficiently store and manage data related to AMM (Automated Market Maker) pools. The RDS Postgres DB should support storing information about newly identified AMM pools, which are updated periodically through a batch job running multiple times a day or once a day.
4. **Database-agnostic Tools**: To maintain database schema and SQL scripts, database-agnostic tools like Liquibase or SQLAlchemy are utilized, ensuring seamless development and management of the RDS Postgres DB. 
5. Additionally, the database design will strive to seamlessly read AMM pool data, primarily utilized by the "arbitrage bot." Given that AMM pool data may not always be transactional, the "arbitrage bot" must employ caching strategies to optimize data retrieval. The chosen RDS Postgres DB will be specifically tailored to support this requirement and ensure efficient data access.

**Data Collection with Amazon Neptune or Network Algorithm:**

In addition to RDS Postgres DB, the project employs **Amazon Neptune** or **Network Algorithm** as a specialized data collection solution. Amazon Neptune or Network Algorithm is responsible for calculating and storing routes (with a maximum depth of 3) between all token pairs. It generates comprehensive route information, which is then stored in JSON format.

**Decoupling of RDS Postgres DB and Neptune:**

RDS Postgres DB and Neptune are not parallel components. Neptune is used in one way for route calculation, while RDS Postgres DB is responsible for storing the calculated route data.

**API Access and Query Functionality:**

The RDS Postgres DB structure enables the development of APIs and query interfaces to expose the stored data to developers and users. This will allow users to access the database using APIs and retrieve the desired information, such as all routes between any two given tokens. The API-based approach ensures efficient data retrieval and enhances the user experience.



## 2. Definitions

**Postgres DB**: RDS Postgres DB is a reliable and scalable relational database suitable for the Definer MevMax project. It efficiently manages data recording, deletion, modification, and querying, meeting the project's data management needs. While Aurora DB offers superior performance, it is not the ideal choice due to specific requirements. RDS Postgres DB meets the requirements of storing newly identified AMM pools through scheduled batch jobs. It also serves as the primary database for the "arbitrage bot" to read AMM pool data efficiently. To optimize data retrieval, the "arbitrage bot" employs caching strategies.

**Liquidity Pool**: A liquidity pool represents a pool of assets where users can deposit their funds to facilitate decentralized trading. It typically contains two types of tokens.

**Protocol**: A protocol is an implementation of a peer-to-peer decentralized trading protocol. Each DEX typically operates on a specific protocol, such as Uniswap, SushiSwap, or PancakeSwap.

**Pairs**: Pairs represent the trading pairs, such as ETH/USDT, indicating the exchange between ETH and USDT.

**Route**: A transaction's route represents the combination of pairs involved in completing a transaction. It defines the path taken to exchange one asset for another.

**Liquidity Token**: In decentralized exchanges (DEXs), users who provide liquidity to liquidity pools receive liquidity tokens. These tokens represent the users' share of assets provided in the liquidity pool. By holding liquidity tokens, users can earn transaction fee rewards. Each protocol is typically associated with a specific liquidity token type. Common liquidity tokens include UNI for Uniswap and SUSHI for SushiSwap. When users deposit liquidity tokens, they usually provide an equal proportion of two assets (50%) as the liquidity pool aims to facilitate the exchange between the two assets.

**TVL**: Total Value Locked in the liquidity pool, representing the total worth of assets in the pool.

**Fee**: When users perform transactions, they are required to pay transaction fees. These fees are primarily used to reward liquidity providers and compensate them for the cost and risks associated with providing liquidity. Transaction fees are typically collected proportionally.

**Coverage of Swaps**: Number of transactions actually captured by MevMax as a percentage of the total number of actual transactions.



## 3. Postgres DB Design

### i. DB Structure (ER Diagram)

![image-20230822161250813](https://p.ipic.vip/nk8603.png)

https://lucid.app/lucidchart/471bbc6c-eafe-4b2a-b2da-54f568999bc9/edit?viewport_loc=8%2C-668%2C1802%2C1279%2C0_0&invitationId=inv_4920e97a-d357-48a0-9f69-2c0ca130f0fd



### ii. Table Structure and Field Description (PostgreSQL standard [Data Types](https://www.postgresql.org/docs/8.1/datatype.html))

1. **Pool Table**:
   - **`pool_id`**: SERIAL PRIMARY KEY. The unique identifier for a liquidity pool.
   - `pool_address`: VARCHAR(50) NOT NULL. The address of the liquidity pool contract on the blockchain. For example: 0x69d6b9a5709eead2c6568c1f636b32707ea55a7e.
   - `protocol_id`: INTEGER (Foreign Key to Protocol table's protocol_id). The ID of the protocol associated with the pool.
   - `blockchain_id`: INTEGER (Foreign Key to Pair table's pair_id). The ID of the blockchain on which the pool exists.
   - `tvl`: NUMERIC. Total Value Locked in the liquidity pool, representing the total worth of assets in the pool.
   - `fee`: NUMERIC(10, 8). The transaction fee charged for trades within the pool, the value is in decimal form, without the percentage symbol (%)..
   - `pool_flag`: BOOLEAN DEFAULT FALSE. A flag indicating whether this pool's data is used in route calculations. It can have either `true` or `false` values. When `flag` is `true`, it means that this pool's data is considered for route calculations. Conversely, when `flag` is `false`, this pool's data is excluded from route calculations.
2. **Protocol Table**:
   - **`protocol_id`**: SERIAL PRIMARY KEY. The unique identifier for a protocol.
   - `protocol_name`: VARCHAR(50) NOT NULL. The name of the protocol. For example: Uniswap.
3. **Blockchain Table**:
   - **`blockchain_id`**: SERIAL PRIMARY KEY. The unique identifier for a blockchain.
   - `blockchain_name`: VARCHAR(50) NOT NULL. The name of the blockchain. For example: Ethereum.
4. **Pair Table**:
   - **`pair_id`**: SERIAL PRIMARY KEY. The unique identifier for a pair.
   - `token1_id`: INTEGER NOT NULL (Foreign Key to Token table's token_id). The ID of one of the tokens in a pair.
   - `token2_id`: INTEGER NOT NULL (Foreign Key to Token table's token_id). The ID of the other token in a pair.
   - `pair_flag`: BOOLEAN DEFAULT TURE. A flag indicating whether this pair's data is used in route calculations.
   - `route_url`: VARCHAR(255). The URL data that provides access to the JSON file containing route information for this pair. The JSON file is stored in an AWS S3 bucket.
5. **Token Table**:
   - **`token_id`**: SERIAL PRIMARY KEY. The unique identifier for a token.
   - `token_symbol`: VARCHAR(100) NOT NULL. The symbol or ticker of the token. For example: ETH.
   - `token1ddress`: VARCHAR(50) NOT NULL. The address of the token on the blockchain. For example: 0x2170ed0880ac9a755fd29b2688956bd959f933f8.
   - `decimal`: INTEGER. The decimal precision of the token.
   - `num_holders`: INTEGER. The count of holders for a specific token.
6. **Pool_Pair Table** (The Pool_Pair Table serves as a bridge table that establishes the relationship between pools and pairs. The bridge table is used to solve the problem of multiple pairs corresponding to multiple pools.):
   - **`pool_pair_id`**: SERIAL PRIMARY KEY. The unique identifier for a pair-token combination.
   - `pool_id`: INTEGER (Foreign Key to Pool table's pool_id, NOT NULL). The ID of the pool.
   - `pair_id`: INTEGER (Foreign Key to Token table's token_id, NOT NULL). The ID of the pair.



* Note: 
  * SERIAL: Used to automatically generate unique values for the primary key column. It starts with 1 and increments by 1 for each new row added to the table. This ensures that each row in the table has a unique identifier.
  * VARCHAR(n): It is a variable-length character data type. n specifies the maximum number of characters the field can hold.
  * INTEGER: It is used to store whole numbers. It is a 32-bit signed integer data type, capable of holding values from -2,147,483,648 to 2,147,483,647.
  * NUMERIC(p, s): It is used to store decimal numbers with exact precision. p specifies the total number of digits (both to the left and right of the decimal point), and s specifies the number of digits to the right of the decimal point. If the NUMERIC data type is used without specifying p and s, it can store decimal numbers with arbitrary precision. For example, NUMERIC(6, 4) means a total of 6 digits with 4 digits to the right of the decimal point.



### iii. Sample Data for the Table

1. **Pool Table**:

| pool_id | pool_address                               | protocol_id | blockchain_id | tvl        | fee    | pool_flag |
| ------- | ------------------------------------------ | ----------- | ------------- | ---------- | ------ | --------- |
| 1       | 0x2e707261d086687470b515b320478eb1c88d49bb | 1           | 1             | 721285.275 | 0.0015 | FALSE     |
| 2       | 0x4f263c2f03d8dcd7dea928de0224e34cebd9f03c | 1           | 1             | 711853.694 | 0.0015 | FALSE     |
| 3       | 0xf65c1c0478efde3c19b49ecbe7acc57bb6b1d713 | 1           | 1             | 559448.303 | 0.0015 | FALSE     |
| 4       | 0x51e6d27fa57373d8d4c256231241053a70cb1d93 | 1           | 1             | 482657.767 | 0.0015 | FALSE     |
| 5       | 0xc087c78abac4a0e900a327444193dbf9ba69058e | 1           | 1             | 462939.845 | 0.0015 | FALSE     |
| 6       | 0x119d6ebe840966c9cf4ff6603e76208d30ba2179 | 1           | 1             | 372332.916 | 0.0015 | FALSE     |
| 7       | 0xe6ff591f818664865ecab584b1fe679dbb4904db | 1           | 1             | 330414.455 | 0.0015 | FALSE     |
| 8       | 0x7bd46f6da97312ac2dbd1749f82e202764c0b914 | 1           | 1             | 301224.73  | 0.0015 | FALSE     |
| 9       | 0x4428ac6ad2be88c59e811f8953da9dd603fc2fea | 1           | 1             | 253438.271 | 0.0015 | FALSE     |
| 10      | 0xe4afd5f206f10bef8775836997edb11a35498854 | 1           | 1             | 208665.817 | 0.0015 | FALSE     |



2. **Protocol Table**:

| protocol_id | protocol_name |
| ----------- | ------------- |
| 1           | apeswap       |
| 2           | biswap        |
| 3           | ellipsis      |
| 4           | pancakeswap   |
| 5           | sushiswap     |
| 6           | sushiswapv3   |
| 7           | uniswap       |
| 8           | thena_fusion  |
| 9           | thena_v1      |
| 10          | knightswap    |
| 11          | nomiswap      |
| 12          | babydoge      |
| 13          | alitaswap     |
| 14          | appleswap     |
| 15          | cafeswap      |
| 16          | cheeseswap    |
| 17          | coinswap      |



3. **BlockChain Table**:

| blockchain_id | blockchain_name |
| ------------- | --------------- |
| 1             | Polygon         |
| 2             | BNB             |
| 3             | ETH             |



4. **Pair Table**:

| pair_id | token0_id | token1_id | route_url | Pair_flag |
| ------- | --------- | --------- | --------- | --------- |
| 1       | 2839      | 7252      | NULL      | TURE      |
| 2       | 1812      | 7252      | NULL      | TURE      |
| 3       | 7252      | 7918      | NULL      | TURE      |
| 4       | 7252      | 8746      | NULL      | TURE      |
| 5       | 1098      | 7252      | NULL      | TURE      |
| 6       | 3206      | 7252      | NULL      | TURE      |
| 7       | 2168      | 7252      | NULL      | TURE      |
| 8       | 965       | 7252      | NULL      | TURE      |
| 9       | 5810      | 7252      | NULL      | TURE      |
| 10      | 2820      | 7252      | NULL      | TURE      |



5. **Token Table**:

| token_id | token_symbol | token_address                              | decimal | num_holders |
| -------- | ------------ | ------------------------------------------ | ------- | ----------- |
| 1        | MNEP         | 0x0b91b07beb67333225a5ba0259d55aee10e3a578 | 8       | 13966866    |
| 2        | USDT         | 0xc2132d05d31c914a87c6611c10748aeb04b58e8f | 6       | 2181284     |
| 3        | USDC         | 0x2791bca1f2de4661ed88a30c99a7a9449aa84174 | 6       | 1880371     |
| 4        | WETH         | 0x7ceb23fd6bc0add59e62ac25578270cff1b9f619 | 18      | 1641787     |
| 5        | DAI          | 0x8f3cf7ad23cd3cadbd9735aff958023239c6a063 | 18      | 1081396     |
| 6        | CP           | 0xf9d3d8b25b95bcda979025b74fdfa7ac3f380f9f | 18      | 1001355     |
| 7        | WBTC         | 0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6 | 8       | 406474      |
| 8        | WMATIC       | 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270 | 18      | 404042      |
| 9        | amUSDC       | 0x1a13f4ca1d028320a707d99520abfefca3998b7f | 6       | 380776      |
| 10       | agEUR        | 0xe0b52e49357fd4daf2c15e02058dce6bc0057db4 | 18      | 294880      |



6. **Pool_Pair Table**:

| pool_pair_id | pool_id | pair_id |
| ------------ | ------- | ------- |
| 1            | 1       | 2096    |
| 2            | 2       | 5277    |
| 3            | 3       | 4686    |
| 4            | 4       | 1559    |
| 5            | 5       | 4716    |
| 6            | 6       | 1431    |
| 7            | 7       | 4101    |
| 8            | 8       | 6159    |
| 9            | 9       | 3462    |
| 10           | 10      | 6141    |



### iv: Table Creation Statements using PostgreSQL

```PostgreSQL
commands = (
        """
        CREATE TABLE IF NOT EXISTS "Protocol" (
            "protocol_id" SERIAL PRIMARY KEY,
            "protocol_name" VARCHAR(50) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS "BlockChain" (
            "blockchain_id" SERIAL PRIMARY KEY,
            "blockchain_name" VARCHAR(50) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS "Token" (
            "token_id" SERIAL PRIMARY KEY,
            "token_symbol" VARCHAR(100) NOT NULL,
            "token_address" VARCHAR(50) NOT NULL,
            "decimal" INTEGER,
            "num_holders" INTEGER
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS "Pair" (
            "pair_id" SERIAL PRIMARY KEY,
            "token0_id" INTEGER NOT NULL REFERENCES "Token"("token_id"),
            "token1_id" INTEGER NOT NULL REFERENCES "Token"("token_id"),
            "routes_url" VARCHAR(255),
            "pair_flag" BOOLEAN DEFAULT TRUE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS "Pool" (
            "pool_id" SERIAL PRIMARY KEY,
            "pool_address" VARCHAR(50) NOT NULL,
            "protocol_id" INTEGER REFERENCES "Protocol"("protocol_id"),
            "blockchain_id" INTEGER REFERENCES "BlockChain"("blockchain_id") NOT NULL,
            "tvl" NUMERIC,
            "fee" NUMERIC(10, 8),
            "pool_flag" BOOLEAN DEFAULT FALSE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS "Pool_Pair" (
            "pool_pair_id" SERIAL PRIMARY KEY,
            "pool_id" INTEGER NOT NULL REFERENCES "Pool"("pool_id"),
            "pair_id" INTEGER NOT NULL REFERENCES "Pair"("pair_id")
        )
        """
    )
```



### v: Data Initialization

#### 1). **Original Data Sample**:

```json
pool_data.json [
  {
    "Name": "apeswap",
    "pool_address": "0x2e707261d086687470b515b320478eb1c88d49bb",
    "tvl": 721285.2754370774,
    "fee": 0.0015,
    "token1": "0x55d398326f99059ff775485246999027b3197955",
    "token1_symbol": "USDT",
    "token1_decimals": 18,
    "token2": "0xe9e7cea3dedca5984780bafc599bd69add087d56",
    "token2_symbol": "BUSD",
    "token2_decimals": 18
  },
  {
    "Name": "apeswap",
    "pool_address": "0x4f263c2f03d8dcd7dea928de0224e34cebd9f03c",
    "tvl": 711853.6943376027,
    "fee": 0.0015,
    "token1": "0x55d398326f99059ff775485246999027b3197955",
    "token1_symbol": "USDT",
    "token1_decimals": 18,
    "token2": "0xd88ca08d8eec1e9e09562213ae83a7853ebb5d28",
    "token2_symbol": "XWIN",
    "token2_decimals": 18
  },
  {
    "Name": "apeswap",
    "pool_address": "0xf65c1c0478efde3c19b49ecbe7acc57bb6b1d713",
    "tvl": 559448.3034668298,
    "fee": 0.0015,
    "token1": "0x603c7f932ed1fc6575303d8fb018fdcbb0f39a95",
    "token1_symbol": "BANANA",
    "token1_decimals": 18,
    "token2": "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
    "token2_symbol": "WBNB",
    "token2_decimals": 18
  },
  {
    "Name": "apeswap",
    "pool_address": "0x51e6d27fa57373d8d4c256231241053a70cb1d93",
    "tvl": 482657.76678633183,
    "fee": 0.0015,
    "token1": "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
    "token1_symbol": "WBNB",
    "token1_decimals": 18,
    "token2": "0xe9e7cea3dedca5984780bafc599bd69add087d56",
    "token2_symbol": "BUSD",
    "token2_decimals": 18
  },
......    
                
token_data.json 
  [
  {
    "address": "0x55d398326f99059ff775485246999027b3197955",
    "symbol": "USDT",
    "decimal": 18,
    "holder": 0
  },
  {
    "address": "0x603c7f932ed1fc6575303d8fb018fdcbb0f39a95",
    "symbol": "BANANA",
    "decimal": 18,
    "holder": 0
  },
  {
    "address": "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
    "symbol": "WBNB",
    "decimal": 18,
    "holder": 0
  },
  {
    "address": "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
    "symbol": "USDC",
    "decimal": 18,
    "holder": 0
  },
  {
    "address": "0x489580eb70a50515296ef31e8179ff3e77e24965",
    "symbol": "RADAR",
    "decimal": 18,
    "holder": 0
  },
......
                 
route_data.json
{
    "blockchain_name": "Polygon",
    "blockchain_id": 1,
    "max_depth": 1,
    "total_pairs": 49141,
    "total_pairs_find_routes": 452,
    "total_pairs_with_null_routes": 48689,
    "generated_on": "2023-08-21 16:00:39.048529",
    "token_pairs": [
        {
            "id": "0xd80b434fe566e4f164f09017258c934dbbffbf6c80c29030ba15640064ce2a21",
            "tokens": [
                "0x55d398326f99059ff775485246999027b3197955",
                "0x2170ed0880ac9a755fd29b2688956bd959f933f8"
            ],
            "routes": {
                "depth = 1": [
                    {
                        "depth": 1,
                        "route_id": 1,
                        "pools": [
                            {
                                "pool_address": "0x5af4859f62edb197887da2919ed185a89771826c",
                                "token0": "0x55d398326f99059ff775485246999027b3197955",
                                "token1": "0x2170ed0880ac9a755fd29b2688956bd959f933f8",
                                "protocol_name": "sushiswap"
                            }
                        ]
                    },
                    {
                        "depth": 1,
                        "route_id": 2,
                        "pools": [
                            {
                                "pool_address": "0x63b30de1a998e9e64fd58a21f68d323b9bcd8f85",
                                "token0": "0x55d398326f99059ff775485246999027b3197955",
                                "token1": "0x2170ed0880ac9a755fd29b2688956bd959f933f8",
                                "protocol_name": "biswap"
                            }
                        ]
                    },
                ]
            }
        },
        {
            "id": "0x698fd28b4acb921d8c31af6a1c57707cc70c6a6702a8c407de61571aee96ee81",
            "tokens": [
                "0x55d398326f99059ff775485246999027b3197955",
                "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82"
            ],
            "routes": {
                "depth = 1": [
                    {
                        "depth": 1,
                        "route_id": 1,
                        "pools": [
                            {
                                "pool_address": "0x7f51c8aaa6b0599abd16674e2b17fec7a9f674a1",
                                "token0": "0x55d398326f99059ff775485246999027b3197955",
                                "token1": "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82",
                                "protocol_name": "pancakeswap"
                            }
                        ]
                    }
                ]
            }
        },
......

```

#### 2). **Data Initialization Steps**:

1. ##### Initialize **BlockChain Table** First.

​	**BlockChain Table**:

- Manually write the sample data for the `BlockChain` table.



2. ##### Initialize **Token Table**.

​	**Token Table**:

- Ensure `token1ddress` is unique in the table.
- For each row in the token_data.json JSON data, check if the token with the given `token1ddress` exists in the table. If it exists, check if `token_symbol` and `decimal` match. If it doesn't, generate a log message and update it with the newly inserted data.



3. ##### **Initialize Pair Table, Pool_Pair Table, Pool Table, and Protocol Table.**

   ###### 3.1 Initialize Pair Table, Pool_Pair Table, Pool Table

   Then, initialize the **Pair Table** to make sure that all tokens are not duplicated. After the **Pair Table** is generated, initialize **Pool_Pair Table** and **Pool Table**.

​	**Pair Table**:

- For each row in the pool_data.json JSON data, create a list of tokens (`token1` to `token8`) that are not `null`.
- For each token in the list, check if the token exists in the `Token` table based on its `token1ddress`. If it exists, get the `token_id`.
- If token1ddress is not in the `Token` table, generate a new `token_id` in `Token` table, insert `token1ddress`, and mark `token_symbol` as "unknown".
- Based on the list of tokens obtained above, do a two-by-two matching, e.g. (token1_id, token2_id), retrieve if the combination (token1_id, token2_id) already exists in the `Pair` Table, and if not, insert a new row in the Pair table.
- Ensure `(token1_id, token2_id)` combination is unique.

​	**Pool Table**:

- Ensure `pool_address` is unique in the table.
- For each row in the pool_data.json  JSON data, check if the pool with the given `pool_address` exists in the table. If it exists, update the other columns (excluding `pool_id`).
- If the pool doesn't exist, insert a new row with the data from the JSON.

​	**Pool_Pair Table**:

- For each row in the pool_data.json JSON data, create a list of tokens (`token1` to `token8`) that are not `null`.  Perform a two-by-two matching, e.g. (token1_id, token2_id), 

- For each pool with the given `pool_address`, find all the pairs associated with it by looking up the `token_id` from the `Pair` table based on the tokens obtained above.

- Insert rows into the `Pool_Pair` table with the `pool_id` and `pair_id` based on the pairs associated with each pool.

  ###### 3.2 Initialize Protocol Table

  In the process of updating the **Pool Table**, read the data in the "name" field of pool_data.json, and if the name is not in the **Protocol Table**, then update a new row of data.

​	**Protocol Table**:

- For the given pool_data.json JSON data with `"Name": "quickswap"`, update the `protocol_name` to "quickswap" (assuming this corresponds to the protocol with `protocol_id` 1).



4. ##### Initialize **Routes** Data

* Read the contents of the route_data.json file.
* For each entry in the JSON data, extract the <token1_address> and <token2_address>.
* Create individual JSON files for each entry with the format <token1_address>_<token2_address>.json.
* Upload these JSON files to an **S3 bucket**, ensuring they are accessible.
* Generate URLs for accessing each uploaded JSON file in the S3 bucket, forming the URLs with the format: <URL> (<token1_id>, <token2_id>).
* Retrieve the token1_id and token2_id from the Pair Table for each pair of <token1_address> and <token2_address>.
* Pair the URLs of the JSON files with their corresponding token1_id and token2_id from the Pair Table, store as routes_url.



### vi: Data Updating

**Data Updating Steps**:

1. Update **Token Table** with new data from `token_data.json`.

2. Update **Pair Table** with new data from `pool_data.json` and ensure the uniqueness of `(token1_id, token2_id)` combination.
3. Update **Pool Table** with new data from `pool_data.json`.
4. Update **Pool_Pair Table** with new data from `pool_data.json`.
5. Check if the `name` from `pool_data.json` exists in the **Protocol Table** and update it accordingly.
6. Update  **Routes** **Data** in **Pair Table** with new data from `route_data.json`.



### vii. Data Calculation

Calculating totals and calculating totals based on **group by blockchains**.

1. Total count of Tokens

```sql
SELECT COUNT(*) AS total_tokens FROM Token;
```

```sql
SELECT b.blockchain_name, COUNT(DISTINCT t.token_id) AS token_count
FROM Pool_Pair pp
JOIN Token t ON pp.pair_id = t.token_id
JOIN Pool p ON pp.pool_id = p.pool_id
JOIN Blockchain b ON p.blockchain_id = b.blockchain_id
GROUP BY b.blockchain_name;
```

2. Total count of Pools

```sql
SELECT COUNT(*) AS total_pools FROM Pool;
```

```sql
SELECT b.blockchain_name, COUNT(p.pool_id) AS pool_count
FROM Pool p
JOIN Blockchain b ON p.blockchain_id = b.blockchain_id
GROUP BY b.blockchain_name;
```

3. Total count of Pairs

```sql
SELECT COUNT(*) AS total_pairs FROM Pair;
```

```sql
SELECT b.blockchain_name, COUNT(pp.pair_id) AS pair_count
FROM Pool_Pair pp
JOIN Pool p ON pp.pool_id = p.pool_id
JOIN Blockchain b ON p.blockchain_id = b.blockchain_id
GROUP BY b.blockchain_name;
```

4. Total TVL recorded in each blockchain

```sql
SELECT b.blockchain_name, SUM(p.tvl) AS total_tvl
FROM Pool p
JOIN Blockchain b ON p.blockchain_id = b.blockchain_id
GROUP BY b.blockchain_name;
```

5. Ranked token pairs

```sql
SELECT
    POOL.pool_address,
    TA.token1_address AS token1_address,
    TB.token2_address AS token2_address
FROM
    "Pair" P
JOIN
    "Pool_Pair" PP ON P.pair_id = PP.pair_id
JOIN
    "Pool" POOL ON PP.pool_id = POOL.pool_id
JOIN
    "Token" TA ON P.token1_id = TA.token_id
JOIN
    "Token" TB ON P.token2_id = TB.token_id;


WITH RankedTokens AS (
    SELECT
        *,
        COALESCE(num_holders, 0) AS num_holders_filled,
        RANK() OVER (ORDER BY COALESCE(num_holders, 0) DESC) AS rank
    FROM
        "Token"
)
, FilteredTokens AS (
    SELECT *
    FROM RankedTokens
    WHERE rank <= 50
)
SELECT DISTINCT
		POOL.pool_address,
    T1.token_address AS token1_address,
    T2.token_address AS token2_address
FROM
    "Pair" P
JOIN
    "Pool_Pair" PP ON P.pair_id = PP.pair_id
JOIN
    "Pool" POOL ON PP.pool_id = POOL.pool_id
JOIN
    FilteredTokens T1 ON P.token1_id = T1.token_id
JOIN
    FilteredTokens T2 ON P.token2_id = T2.token_id
WHERE
		POOL.tvl > 1000;
    




```

6. Update"flag" data based on "tvl"

```sql
UPDATE "Pool"
SET "flag" = true
WHERE "tvl" > 50000;
```



### viii: Follow-up Optimization

1. Build Indexes

   * Pool_Pair Table: create a union index for pool_id and pair_id.

```sql
CREATE INDEX token1_token2_pairs_index ON Pool_Pairs (token1_id, token2_id);
```

2. ......



### ix: Issues Encountered



CREATE INDEX token1_token2_pairs_index ON Pool_Pairs (token1_id, token2_id);

	2	......

viii: Issues Encountered



# 4. Code Structure

## i. main

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



## ii. generate_original_data package

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



## iii. init_mevmax_db package

```
├── init_mevmax_db/
│   ├── __init__.py
│   ├── create_mevmax_db.py
│   ├── init_blockchain_data.py
│   ├── init_token_data.py
│   ├── init_pair_data.py
│   └── init_pool_protocol_poolpair_data.py
```

3.1 init_pair_data.py

* During the initialization of pairs in the database, pairs are established based on the `number_holders` for each token. This process involves determining a list of tokens and subsequently forming pairs by combining them pairwise, resulting in the creation of pair IDs.

3.2 init_pool_protocol_poolpair_data.py

* While initializing pools, the parameter `pool_flag` is updated based on the value of Total Value Locked (TVL).



## iv. update_mevmax_db package

```
├── update_mevmax_db/
│   ├── __init__.py
│   ├── update_blockchain_data.py
│   ├── update_token_pair_data.py
│   └── update_pool_protocol_poolpair_data.py
```

4.2 update_token_pair_data.py

* When updating the tokens in the database, a new list of tokens to be inserted is determined based on the number of holders for each token.

* During the process of updating pairs, tokens that meet certain criteria (determined by `number_holders`) are filtered from the database. These selected tokens are then combined in pairs with tokens from the newly inserted token list, resulting in the creation of pair sets and their associated pair IDs.

4.1 update_pool_protocol_poolpair_data.py

* While updating pools, the parameter `pool_flag` is adjusted based on the Total Value Locked (TVL) value.



## v. cal_route_data package

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
* When calculating routes, pools are filtered based on Total Value Locked (TVL), and pairs are determined based on the number of token holders. This process involves utilizing SQL parameters to introduce additional logic for filtering pairs.



## vi. statistical_analysis_data package

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



## vii. config

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
- `tvl_pool_flag`: Logical parameter to initialize/update `pool_flag` in the "Pool" table in the database.
- `holders_pair_flag`: Logical parameter to initialize/update `pair_flag` in the "Pair" table in the database.

7.3 **[MAIN] Section**:

- `op_type`: Determines the operation type. It can either be `init` (for initialization) or `update`.

7.4 **[DATABASE] Section**:

- Contains database connection details:
  - `user`: The username for the database.
  - `password`: The password associated with the user.
  - `host`: The host address of the database server.
  - `database`: The name of the database.



# 5. Install Package

```
# This is a requirements file for the MevMax project.
# It lists the required packages for the MevMax project to run successfully.
# pip install -r requirements.txt

# Database
psycopg2

# Numerical computation
numpy
pandas

# Network analysis
networkx

# Ethereum blockchain interactions
web3

# Progress bar
tqdm

# GraphQL API
gql
# please install the requests-toolbelt package Manually
```



# 6. Routes Data Format

**Blockchain Name:** The name of the blockchain being documented.

**Blockchain ID:** An identifier for the blockchain.

**Max Depth:** The maximum depth considered in the file.

**Total Pairs:** The total number of token pairs included in the file.

**Generated On:** Timestamp indicating when the file was generated.

**Token Pairs:** This section comprises an array of objects, each representing a token pair and its associated routes.

For each token pair:

- **ID:** An identifier generated using the keccak-256 hash of the concatenated addresses of the two tokens in the pair.

  ```
  id_hash = Web3().keccak(text=token0_address + token1_address)
  ```

- **Tokens:** An array containing the addresses of the two tokens forming the pair.

- **Routes:** This property contains arrays of objects representing routes at different depth levels. Each depth level contains an array of routes.

- For each routes:
  - **Depth X Routes:** Arrays containing routes at depth X, where X represents the depth level.
  - For each route:
    - **Depth:** The depth level of the route, indicating the number of intermediary pools.
    - **Route ID:** An auto-incremented identifier for each individual route within a specific depth level.
    - **Pools**: An array of objects representing the sequence of pools and tokens that constitute the route between the two tokens.
    - For each Pools:
      - **Pool Address:** The address of the pool.
      - **Token 0:** The address of one of the tokens in the pool.
      - **Token 1**: The address of the other token in the pool.
      - **Protocol Name:** The name of the protocol that the pool is part of.



## i. data_format

```json
{
  "blockchain_name": "The name of the blockchain",
  "blockchain_id": "The id of the blockchain",
  "max_depth": "The maximum depth considered in the file",
  "total_pairs": "Total number of pairs included in the file",
	"total_pairs_find_routes": "Total number of pairs for which routes were successfully found",
	"total_pairs_with_null_routes": "Total number of pairs with null or unavailable routes"
  "generated_on": "Timestamp of when the file was generated",
  "token_pairs": [
    {
      "id": "keccak-256 hash of “tokenA address”+”tokenB address”",
      "tokens": ["tokenA address", "tokenB address"],
      "routes": {
        "depth = 1": [
          {
            "depth": 1,
            "route_id": 1,
            "pools": [
              // With depth = 1, the list of pools has one element,
              // the token0 and the token1 of the element are ["tokenA address", "tokenB address"]
              {
                "pool_address": "pool’s address",
                "token0": "pool’s token0 address.",
                "token1": "pool’s token1 address.",
                "protocol_name": "the name of the protocol that the pool is part of"
              }
            ]
          },
          {
            "depth": 1,
            "route_id": 2,
            "pools": [
              {
                "pool_address": "pool’s address",
                "token0": "pool’s token0 address.",
                "token1": "pool’s token1 address.",
                "protocol_name": "the name of the protocol that the pool is part of"
              }
            ]
          },
          // ...
        ],
        "depth = 2": [
          {
            "depth": 2,
            "route_id": 1,
            "pools": [
              // With depth = 2, the list of pools has two elements,
              // the token0 of the first element and the token1 of the last element are ["tokenA address", "tokenB address"] in the pair,
              // and the token1 of the first element is the token0 of the last element.
              {
                "pool_address": "pool’s address",
                "token0": "pool’s token0 address.",
                "token1": "pool’s token1 address.",
                "protocol_name": "the name of the protocol that the pool is part of"
              },
              {
                "pool_address": "pool’s address",
                "token0": "pool’s token0 address.",
                "token1": "pool’s token1 address.",
                "protocol_name": "the name of the protocol that the pool is part of"
              }
            ]
          },
          // ...
        ],
        "depth = 3": [
          {
            "depth": 3,
            "route_id": 1,
            "pools": [
              // With depth = 3, the list of pools has three elements,
              // the token0 of the first element and the token1 of the last element are ["tokenA address", "tokenB address"] in the pair,
              // and the token1 of the first element is the token0 of the last element.
              {
                "pool_address": "pool’s address",
                "token0": "pool’s token0 address.",
                "token1": "pool’s token1 address.",
                "protocol_name": "the name of the protocol that the pool is part of"
              },
              {
                "pool_address": "pool’s address",
                "token0": "pool’s token0 address.",
                "token1": "pool’s token1 address.",
                "protocol_name": "the name of the protocol that the pool is part of"
              },
              {
                "pool_address": "pool’s address",
                "token0": "pool’s token0 address.",
                "token1": "pool’s token1 address.",
                "protocol_name": "the name of the protocol that the pool is part of"
              }
            ]
          },
          // ...
        ],
        // ...
      }
    },
    // ...
  ]
}
```



## ii. data_input_example

```
pool_data.json [
  {
    "Name": "apeswap",
    "pool_address": "0x2e707261d086687470b515b320478eb1c88d49bb",
    "tvl": 721285.2754370774,
    "fee": 0.0015,
    "token1": "0x55d398326f99059ff775485246999027b3197955",
    "token1_symbol": "USDT",
    "token1_decimals": 18,
    "token2": "0xe9e7cea3dedca5984780bafc599bd69add087d56",
    "token2_symbol": "BUSD",
    "token2_decimals": 18
  },
  {
    "Name": "apeswap",
    "pool_address": "0x4f263c2f03d8dcd7dea928de0224e34cebd9f03c",
    "tvl": 711853.6943376027,
    "fee": 0.0015,
    "token1": "0x55d398326f99059ff775485246999027b3197955",
    "token1_symbol": "USDT",
    "token1_decimals": 18,
    "token2": "0xd88ca08d8eec1e9e09562213ae83a7853ebb5d28",
    "token2_symbol": "XWIN",
    "token2_decimals": 18
  },
  {
    "Name": "apeswap",
    "pool_address": "0xf65c1c0478efde3c19b49ecbe7acc57bb6b1d713",
    "tvl": 559448.3034668298,
    "fee": 0.0015,
    "token1": "0x603c7f932ed1fc6575303d8fb018fdcbb0f39a95",
    "token1_symbol": "BANANA",
    "token1_decimals": 18,
    "token2": "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
    "token2_symbol": "WBNB",
    "token2_decimals": 18
  },
  {
    "Name": "apeswap",
    "pool_address": "0x51e6d27fa57373d8d4c256231241053a70cb1d93",
    "tvl": 482657.76678633183,
    "fee": 0.0015,
    "token1": "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
    "token1_symbol": "WBNB",
    "token1_decimals": 18,
    "token2": "0xe9e7cea3dedca5984780bafc599bd69add087d56",
    "token2_symbol": "BUSD",
    "token2_decimals": 18
  },
......    
```



## iii. intermediate_data_example

pool_data.csv

| pool_address                               | tvl        | blockchain_name | blockchain_id | protocol_name | protocol_id | token0_address                             | token1_address                             |
| ------------------------------------------ | ---------- | --------------- | ------------- | ------------- | ----------- | ------------------------------------------ | ------------------------------------------ |
| 0x003c8bc1fedc20aa7137ed258d09692b1abef812 | 9332.40639 | Polygon         | 1             | pancakeswap   | 4           | 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c | 0xee89bd9af5e72b19b48cac3e51acde3a09a3ade3 |
| 0x00cd6ae0b289dd0b9bfe269aa231c98503083d1a | 14535.7298 | Polygon         | 1             | sushiswap     | 5           | 0x55d398326f99059ff775485246999027b3197955 | 0xa0ed3c520dc0632657ad2eaaf19e26c4fd431a84 |
| 0x011077b8199cab999e895aed7f8a78755a678106 | 83521.7296 | Polygon         | 1             | thena_fusion  | 8           | 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c | 0xb64e280e9d1b5dbec4accedb2257a87b400db149 |
| 0x0137a5ba1dfa5d6d9a5896251f3d06b2e6669c3a | 9173.68081 | Polygon         | 1             | thena_fusion  | 8           | 0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d | 0x8af48050534ee9bde12ec6e45ea3db4908c04777 |
| 0x01b30a28d66759206b4261765dd5f875b2f877a2 | 2466.91937 | Polygon         | 1             | pancakeswap   | 4           | 0xe9e7cea3dedca5984780bafc599bd69add087d56 | 0xf4bcc7b718e027e1b110ea965e5a0d41c2bc5963 |
| 0x023b6298e2f9ae728b324757599f2a36e002a55a | 3366.35224 | Polygon         | 1             | uniswap       | 7           | 0x55d398326f99059ff775485246999027b3197955 | 0xba2ae424d960c26247dd6c32edc70b295c744c43 |
| 0x0314850b4f776aa8522bcd0bcc13993ac1ba835b | 24194.8465 | Polygon         | 1             | pancakeswap   | 4           | 0x55d398326f99059ff775485246999027b3197955 | 0xe632000238abd0c7c95c116c47bbcc036ecd98ee |
| 0x032ab4727d496b15ecbaca2b5b26f512e4ff7569 | 4460.62353 | Polygon         | 1             | pancakeswap   | 4           | 0x55d398326f99059ff775485246999027b3197955 | 0xde3ab7aa2964fa7e2e45321dc0ee613e854623fc |
| 0x0360b602d953a4787fc0d54fb96c1cadbf57c984 | 18940.9985 | Polygon         | 1             | apeswap       | 1           | 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c | 0x8623e66bea0dce41b6d47f9c44e806a115babae0 |

pools_SQL:

```
WITH RankedTokens AS (
    SELECT
        *,
        COALESCE(num_holders, 0) AS num_holders_filled,
        RANK() OVER (ORDER BY COALESCE(num_holders, 0) DESC) AS rank
    FROM
        "Token"
)
, FilteredTokens AS (
    SELECT *
    FROM RankedTokens
    WHERE num_holders >= 500
)
SELECT DISTINCT
	POOL.pool_address,
	POOL.tvl,
	B.blockchain_name,
	B.blockchain_id,
	Pr.protocol_name,
	Pr.protocol_id,
    T1.token_address AS token1_address,
    T2.token_address AS token2_address
FROM
    "Pair" P
JOIN
    "Pool_Pair" PP ON P.pair_id = PP.pair_id
JOIN
    "Pool" POOL ON PP.pool_id = POOL.pool_id
JOIN
    FilteredTokens T1 ON P.token1_id = T1.token_id
JOIN
    FilteredTokens T2 ON P.token2_id = T2.token_id
JOIN
    "BlockChain" B ON POOL.blockchain_id = B.blockchain_id
JOIN
    "Protocol" Pr ON POOL.protocol_id = Pr.protocol_id
WHERE
	POOL.tvl >= 2000
```



pairs_sql:

```
SELECT DISTINCT
    T1.token_address AS token1_address,
    T2.token_address AS token2_address
FROM
    "Pair" P
JOIN
    "Token" T1 ON P.token1_id = T1.token_id
JOIN
    "Token" T2 ON P.token2_id = T2.token_id
WHERE
	T1.num_holders > 500 and T2.num_holders > 500
;
```

pair_data.csv

| pair_id | token0_address                             | token1_address                             |
| ------- | ------------------------------------------ | ------------------------------------------ |
| 1       | 0xe9e7cea3dedca5984780bafc599bd69add087d56 | 0x734548a9e43d2d564600b1b2ed5be9c2b911c6ab |
| 2       | 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c | 0x1dc5779ed65bcc1f0a42d654444fb0ce59d7783b |
| 3       | 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c | 0xcbe85589ac839b497c8daecbff79d1a552e27ff2 |
| 4       | 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c | 0xf8a0bf9cf54bb92f17374d9e9a321e6a111a51bd |
| 5       | 0x52f24a5e03aee338da5fd9df68d2b6fae1178827 | 0xf7de7e8a6bd59ed41a4b5fe50278b3b7f31384df |
| 6       | 0x55d398326f99059ff775485246999027b3197955 | 0x18e0662b2da216bc06aa3abcd5ceae88f372198b |
| 7       | 0x2170ed0880ac9a755fd29b2688956bd959f933f8 | 0x1d2f0da169ceb9fc7b3144628db156f3f6c60dbe |
| 8       | 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c | 0x06fda0758c17416726f77cb11305eac94c074ec0 |
| 9       | 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c | 0x3ee2200efb3400fabb9aacf31297cbdd1d435d47 |
| 10      | 0x55d398326f99059ff775485246999027b3197955 | 0xa045e37a0d1dd3a45fefb8803d22457abc0a728a |
| 11      | 0xe9e7cea3dedca5984780bafc599bd69add087d56 | 0x71be881e9c5d4465b3fff61e89c6f3651e69b5bb |



## iv. data_output_example

```
route_data.json
{
    "blockchain_name": "Polygon",
    "blockchain_id": 1,
    "max_depth": 1,
    "total_pairs": 49141,
    "total_pairs_find_routes": 452,
    "total_pairs_with_null_routes": 48689,
    "generated_on": "2023-08-21 16:00:39.048529",
    "token_pairs": [
        {
            "id": "0xd80b434fe566e4f164f09017258c934dbbffbf6c80c29030ba15640064ce2a21",
            "tokens": [
                "0x55d398326f99059ff775485246999027b3197955",
                "0x2170ed0880ac9a755fd29b2688956bd959f933f8"
            ],
            "routes": {
                "depth = 1": [
                    {
                        "depth": 1,
                        "route_id": 1,
                        "pools": [
                            {
                                "pool_address": "0x5af4859f62edb197887da2919ed185a89771826c",
                                "token0": "0x55d398326f99059ff775485246999027b3197955",
                                "token1": "0x2170ed0880ac9a755fd29b2688956bd959f933f8",
                                "protocol_name": "sushiswap"
                            }
                        ]
                    },
                    {
                        "depth": 1,
                        "route_id": 2,
                        "pools": [
                            {
                                "pool_address": "0x63b30de1a998e9e64fd58a21f68d323b9bcd8f85",
                                "token0": "0x55d398326f99059ff775485246999027b3197955",
                                "token1": "0x2170ed0880ac9a755fd29b2688956bd959f933f8",
                                "protocol_name": "biswap"
                            }
                        ]
                    },
                ]
            }
        },
        {
            "id": "0x698fd28b4acb921d8c31af6a1c57707cc70c6a6702a8c407de61571aee96ee81",
            "tokens": [
                "0x55d398326f99059ff775485246999027b3197955",
                "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82"
            ],
            "routes": {
                "depth = 1": [
                    {
                        "depth": 1,
                        "route_id": 1,
                        "pools": [
                            {
                                "pool_address": "0x7f51c8aaa6b0599abd16674e2b17fec7a9f674a1",
                                "token0": "0x55d398326f99059ff775485246999027b3197955",
                                "token1": "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82",
                                "protocol_name": "pancakeswap"
                            }
                        ]
                    }
                ]
            }
        },
......
```

