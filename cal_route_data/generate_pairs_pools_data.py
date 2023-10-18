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


def get_pairs(connection, min_holders, limit, offset):
    # Get a group of pairs for computing routes, use limit and offset to specify the part to extract
    # ORDER BY is necessary
    with connection.cursor() as cursor:
        search_command = f"""
            SELECT P.pair_address, T1.token_address, T2.token_address
            FROM "Pair" P
            JOIN "Token" T1 ON P.token1_address = T1.token_address
            JOIN "Token" T2 ON P.token2_address = T2.token_address
            WHERE T1.num_holders >= {min_holders}
            AND T2.num_holders >= {min_holders}
            ORDER BY P.pair_address
            LIMIT {limit} OFFSET {offset}
        """
        cursor.execute(search_command)
        return cursor.fetchall()


def get_pool_pairs(connection, min_tvl, min_holder):
    with connection.cursor() as cursor:
        # Get all pools and the tokens included by them filtered by min_tvl
        search_command = f"""
            SELECT DISTINCT PO.pool_address, PO.protocol_name, PT1.token_address, PT2.token_address
            FROM "Pool_Token" PT1
            JOIN "Pool_Token" PT2 ON PT1.pool_address = PT2.pool_address AND PT1.token_address < PT2.token_address
            JOIN "Pool" PO ON PO.pool_address = PT1.pool_address
            WHERE PO.tvl >= {min_tvl}
        """
        cursor.execute(search_command)
        graph_draft = cursor.fetchall()
        # Select the number of pairs that will be computed this time
        cursor.execute(f"""
            SELECT COUNT(T1.token_address)
            FROM "Token" T1
            JOIN "Token" T2 ON T1.token_address < T2.token_address
            WHERE T1.num_holders >= {min_holder} AND T2.num_holders >= {min_holder}
        """)
        print("Graph Data Collected")
        return graph_draft, cursor.fetchone()[0]


def get_blockchain_name(connection):
    # In general, the blockchain name of this calculation should be the same as database's name, just in case.
    with connection.cursor() as cursor:
        cursor.execute('SELECT blockchain_name FROM "Pool" LIMIT 1')
        return cursor.fetchone()[0]


"""
    All Code Functions below are useless
"""


def connect_db(user, password, host, database, port=5432):
    """
    Connect to the Postgres database
    """
    connection = psycopg2.connect(user=user, password=password, host=host, dbname=database, port=port)
    return connection


def query_to_csv(connection, sql, file_name):
    """
    Execute SQL query and output results to a CSV file
    """
    cursor = connection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()

    # return rows too. Make sure return them in pd.DataFrame

    with open(file_name, 'w') as f:
        writer = csv.writer(f)
        writer.writerow([i[0] for i in cursor.description])
        writer.writerows(rows)


# Try to transport the config outside as a parameter. Or meaningless waste
# The database related parameters has already been read. It can be transport into this function
# connect_db should be deleted
# Is Limit necessary?? should we use any other more meaningful limits except num_holders???
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
    conn = connect_db(user, password, host, database, port=5433)

    # Set output directory
    output_dir = os.path.join(CAL_ROUTE_DIR, 'pairs_pool_data')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Build file paths
    pair_file = os.path.join(output_dir, pair_file)
    pool_file = os.path.join(output_dir, pool_file)

    # Generate pair data
    # If we can delete the pairs which token becomes "useless", the following query will be easier
    # Should this add an "order by" if use Limit???
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
            SELECT DISTINCT P.pair_address AS pair_id, T1.token_address AS token0_address, T2.token_address AS token1_address
            FROM "Pair" P
            JOIN "Token" T1 ON P.token1_address = T1.token_address
            JOIN "Token" T2 ON P.token2_address = T2.token_address
            WHERE T1.num_holders >= {num_holders}
            AND T2.num_holders >= {num_holders};
            """

    query_to_csv(conn, pair_sql, pair_file)

    # Generate pool data
    # Try this with MySQL to generate temp table. However, such "temp" table must be created with data already in DB
    # For a single pool, will it has more than one blockchain or protocol????
    # If no, the big "SELECT DISTINCT" part can be divided again
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
          POOL.pool_address, POOL.tvl, POOL.blockchain_name,
          POOL.protocol_name, 
          PP.token1_address AS token0_address, PP.token2_address AS token1_address
        FROM "Pool_Pair" PP
        JOIN "Pool" POOL ON PP.pool_address = POOL.pool_address
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
