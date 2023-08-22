"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import configparser

import psycopg2
import csv
import os

# Set the current directory for the routes file
CAL_ROUTE_DIR = os.path.dirname(os.path.abspath(__file__))


def connect_db(user, password, host, database):
    """
    Connect to the Postgres database
    """
    connection = psycopg2.connect(user=user, password=password, host=host, dbname=database)
    return connection


def query_to_csv(connection, sql, file_name):
    """
    Execute SQL query and output results to a CSV file
    """
    cursor = connection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()

    with open(file_name, 'w') as f:
        writer = csv.writer(f)
        writer.writerow([i[0] for i in cursor.description])
        writer.writerows(rows)


def generate_csv_files(pool_file='pool_data.csv', pair_file='pair_data.csv', num_holders=500, limit=1000, min_tvl=2000):
    """
    Generate CSV files containing pool and pair data
    """

    # Read config file
    config = configparser.ConfigParser()
    base_path = os.path.dirname(os.path.abspath(__file__))
    ini_path = os.path.join(base_path, "..", "config", "mevmax_config.ini")
    config.read(ini_path)

    # Get database credentials
    user = config.get('DATABASE', 'user')
    password = config.get('DATABASE', 'password')
    host = config.get('DATABASE', 'host')
    database = config.get('DATABASE', 'database')

    # Connect to database
    conn = connect_db(user, password, host, database)

    # Set output directory
    output_dir = os.path.join(CAL_ROUTE_DIR, 'pairs_pool_data')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Build file paths
    pair_file = os.path.join(output_dir, pair_file)
    pool_file = os.path.join(output_dir, pool_file)

    # Generate pair data
    if limit >= 0:
        pair_sql = \
            f"""
            SELECT DISTINCT P.pair_id, T1.token_address AS token0_address, T2.token_address AS token1_address
            FROM "Pair" P
            JOIN "Token" T1 ON P.token0_id = T1.token_id 
            JOIN "Token" T2 ON P.token1_id = T2.token_id
            WHERE T1.num_holders >= {num_holders}  
            AND T2.num_holders >= {num_holders} 
            LIMIT {limit};
            """
    else:
        pair_sql = \
            f"""
            SELECT DISTINCT P.pair_id, T1.token_address AS token0_address, T2.token_address AS token1_address
            FROM "Pair" P
            JOIN "Token" T1 ON P.token0_id = T1.token_id
            JOIN "Token" T2 ON P.token1_id = T2.token_id
            WHERE T1.num_holders >= {num_holders}
            AND T2.num_holders >= {num_holders};
            """

    query_to_csv(conn, pair_sql, pair_file)

    # Generate pool data
    pool_sql = \
        f"""
        WITH RankedTokens AS (
          SELECT *, RANK() OVER (ORDER BY num_holders DESC) AS rank
          FROM "Token" 
        ), FilteredTokens AS (
          SELECT * FROM RankedTokens  
          WHERE num_holders >= {num_holders}
        )
        SELECT DISTINCT
          POOL.pool_address, POOL.tvl, B.blockchain_name, B.blockchain_id,
          Pr.protocol_name, Pr.protocol_id, 
          T1.token_address AS token0_address, T2.token_address AS token1_address
        FROM "Pair" P
        JOIN "Pool_Pair" PP ON P.pair_id = PP.pair_id
        JOIN "Pool" POOL ON PP.pool_id = POOL.pool_id
        JOIN FilteredTokens T1 ON P.token0_id = T1.token_id
        JOIN FilteredTokens T2 ON P.token1_id = T2.token_id
        JOIN "BlockChain" B ON POOL.blockchain_id = B.blockchain_id
        JOIN "Protocol" Pr ON POOL.protocol_id = Pr.protocol_id
        WHERE POOL.tvl >= {min_tvl}
        """

    query_to_csv(conn, pool_sql, pool_file)


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--pool_file', default='pool_data.csv')
#     parser.add_argument('--pair_file', default='pair_data.csv')
#     parser.add_argument('--num_holders', type=int, default=0)
#     parser.add_argument('--limit', type=int, default=1000)
#     parser.add_argument('--min_tvl', type=int, default=2000)
#     args = parser.parse_args()
#
#     generate_csv_files(args.pool_file, args.pair_file, args.num_holders, args.limit, args.min_tvl)