"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import psycopg2

def create_connection(user, password, host, database):
    """
    Creates a connection to the PostgreSQL database.

    Args:
        user (str): The username for the database.
        password (str): The password for the database.
        host (str): The host address of the database server.
        database (str): The name of the database.

    Returns:
        psycopg2.extensions.connection: The database connection object.
    """
    try:
        connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            dbname=database
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
        """,
    )

    try:
        cursor = connection.cursor()
        for command in commands:
            cursor.execute(command)
        cursor.close()
        connection.commit()
        print("Tables dropped successfully!")
    except (Exception, psycopg2.Error) as error:
        print("Error while dropping PostgreSQL tables:", error)

def create_tables(connection):
    """
    Creates the necessary tables in the PostgreSQL database.

    Args:
        connection (psycopg2.extensions.connection): The database connection object.
    """
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

    try:
        cursor = connection.cursor()
        for command in commands:
            cursor.execute(command)
        cursor.close()
        connection.commit()
        print("Tables created successfully!")
    except (Exception, psycopg2.Error) as error:
        print("Error while creating PostgreSQL tables:", error)

# if __name__ == "__main__":
#     connection = create_connection()
#     if connection:
#         drop_tables(connection)  # Drop existing tables and constraints
#         create_tables(connection)  # Create new tables
#         connection.close()