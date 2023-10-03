"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""
import os

import psycopg2
import configparser


class ConfigReader:

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(ConfigReader, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.config = configparser.ConfigParser()
        base_path = os.path.dirname(os.path.abspath(__file__))
        ini_path = os.path.join(base_path, "..", "config", "mevmax_config.ini")
        self.config.read(ini_path)

    def getDatabaseInfo(self):
        return self.config.get('DATABASE', 'user'), self.config.get('DATABASE', 'password'), \
               self.config.get('DATABASE', 'host'), self.config.get('DATABASE', 'database'), \
               self.config.get('DATABASE', 'port')

    def getInitInfo(self):
        return self.config.get('INIT_DB', 'token_data_path'), self.config.get('INIT_DB', 'pool_data_path'), \
               self.config.get('INIT_DB', 'blockchain_name'), self.config.get('INIT_DB', 'tvl_pool_flag'), \
               self.config.get('INIT_DB', 'holders_pair_flag')

    def getUpdateInfo(self):
        return self.config.get('UPDATE_DB', 'token_data_path'), self.config.get('UPDATE_DB', 'pool_data_path'), \
               self.config.get('UPDATE_DB', 'blockchain_name'), self.config.get('UPDATE_DB', 'tvl_pool_flag'), \
               self.config.get('UPDATE_DB', 'holders_pair_flag')

    def getRouteInfo(self):
        return self.config.getint('GET_ROUTE_DATA', 'num_holders'), \
               self.config.getint('GET_ROUTE_DATA', 'pairs_limit'), self.config.getint('GET_ROUTE_DATA', 'min_tvl'), \
               self.config.getint('GET_ROUTE_DATA', 'depth_limit'), \
               self.config.getint('GET_ROUTE_DATA', 'num_processes'), \
               self.config.getint('GET_ROUTE_DATA', 'max_route'), self.config.getint('GET_ROUTE_DATA', 'chunk_size')


def create_connection(user, password, host, database, port=5432):
    """
    Creates a connection to the PostgreSQL database.

    :param database: string, The name of the database.
    :param host: string, The host address of the database server.
    :param user: string, The username for the database.
    :param password: string, The password for the database.
    :param port: integer, The port of database. Default is 5432

    Returns:
        psycopg2.extensions.connection: The database connection object.
    """
    try:
        connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            dbname=database,
            port=port
        )
        return connection
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)
        return None


def drop_tables(connection):
    """
    Drops the tables in the PostgreSQL database.

    Args:
        connection (psycopg2.extensions.connection): The database connection object.
    """
    commands = (
        """
        DROP TABLE IF EXISTS "Pool_Token"
        """,
        """
        DROP TABLE IF EXISTS "Pool_Pair"
        """,
        """
        DROP TABLE IF EXISTS "Pool"
        """,
        """
        DROP TABLE IF EXISTS "Pair"
        """,
        """
        DROP TABLE IF EXISTS "Token"
        """,
        """
        DROP TABLE IF EXISTS "BlockChain"
        """,
        """
        DROP TABLE IF EXISTS "Protocol"
        """,
        """
        DROP TABLE IF EXISTS "token"
        """,
        """
        DROP TABLE IF EXISTS "pool"
        """
    )
    with connection.cursor() as cursor:
        for command in commands:
            cursor.execute(command)
        connection.commit()
        print("Tables dropped successfully!")

    # try:
    #     cursor = connection.cursor()
    #     for command in commands:
    #         cursor.execute(command)
    #     cursor.close()
    #     connection.commit()
    #     print("Tables dropped successfully!")
    # except (Exception, psycopg2.Error) as error:
    #     print("Error while dropping PostgreSQL tables:", error)


def create_tables(connection):
    """
    Creates the necessary tables in the PostgreSQL database.

    Args:
        connection (psycopg2.extensions.connection): The database connection object.
    """
    with connection.cursor() as cursor:
        # Create "Protocol"
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "Protocol" (
                "protocol_name" VARCHAR(50) PRIMARY KEY
            )
        """)
        # Create "BlockChain"
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "BlockChain" (
                "blockchain_name" VARCHAR(50) PRIMARY KEY
            )
        """)
        # Create "Token"
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "Token" (
                "token_address" VARCHAR(50) PRIMARY KEY,
                "token_symbol" VARCHAR(100) NOT NULL,
                "decimal" INTEGER,
                "num_holders" INTEGER,
                "is_new" BOOLEAN DEFAULT TRUE
            )
        """)
        # Create "Pair"
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "Pair" (
                "pair_address" VARCHAR(100) PRIMARY KEY,
                "token1_address" VARCHAR(50) REFERENCES "Token"("token_address"),
                "token2_address" VARCHAR(50) REFERENCES "Token"("token_address"),
                "routes_data" VARCHAR(255),
                "pair_flag" BOOLEAN DEFAULT FALSE
            )
        """)
        # Create "Pool"
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "Pool" (
                "pool_address" VARCHAR(50) PRIMARY KEY ,
                "protocol_name" VARCHAR(50) REFERENCES "Protocol"("protocol_name"),
                "blockchain_name" VARCHAR(50) REFERENCES "BlockChain"("blockchain_name") NOT NULL,
                "tvl" NUMERIC,
                "fee" NUMERIC(10, 8),
                "pool_flag" BOOLEAN DEFAULT FALSE
            )
        """)
        # Create "Pool_Pair"
        cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS "Pool_Token" (
                "pool_address" VARCHAR(50) NOT NULL REFERENCES "Pool"("pool_address"),
                "token_address" VARCHAR(50) NOT NULL REFERENCES "Token"("token_address"),
                PRIMARY KEY (pool_address, token_address)
            )
        """)
        connection.commit()
        print("Tables created successfully!")

    # try:
    #     cursor = connection.cursor()
    #     for command in commands:
    #         cursor.execute(command)
    #     cursor.close()
    #     connection.commit()
    #     print("Tables created successfully!")
    # except (Exception, psycopg2.Error) as error:
    #     print("Error while creating PostgreSQL tables:", error)

# if __name__ == "__main__":
#     connection = create_connection()
#     if connection:
#         drop_tables(connection)  # Drop existing tables and constraints
#         create_tables(connection)  # Create new tables
#         connection.close()
