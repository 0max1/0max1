"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""
from init_mevmax_db import create_connection
from init_mevmax_db.create_mevmax_db import ConfigReader
from main import init_db_main, update_db_main, get_route_data_main, get_row_data_main
from datetime import datetime
from main.update_db import update_db


def check_missing_pair():
    ini_reader = ConfigReader()
    user, password, host, database, port = ini_reader.getDatabaseInfo()
    connection = create_connection(user, password, host, database, port=port)
    pair_add = '0x5029491e487b357d2f879217ad4c224a0b9b3d21ad027486c1c68e6d3b4edbf8'
    command_get_tokens = f"""
        select token1_address, token2_address from "Pair" where pair_address = '{pair_add}'
    """
    with connection.cursor() as cursor:
        cursor.execute(command_get_tokens)
        results = cursor.fetchall()
        if len(results) == 0:
            print("Low holders. Need check")
            return
        token1, token2 = results[0]
        command_depth_1 = f"""
                select PT1.token_address as token1, PT1.pool_address as pool1, P1.tvl as p1_tvl, PT2.token_address as token2
                from "Pool_Token" PT1
                join "Pool_Token" PT2 on PT1.pool_address = PT2.pool_address
                join "Pool" P1 on PT1.pool_address = P1.pool_address
                where PT1.token_address = '{token1}' and PT2.token_address = '{token2}'
            """
        command_depth_2 = f"""
                select PT1.token_address as token1, PT2.pool_address as pool1, P1.tvl as p1_tvl, 
                PT2.token_address as temp1, PT3.pool_address as pool2, P2.tvl as p2_tvl, PT4.token_address as token2
                from "Pool_Token" PT1
                join "Pool_Token" PT2 on PT1.pool_address = PT2.pool_address and PT1.token_address <> PT2.token_address
                join "Pool_Token" PT3 on PT2.token_address = PT3.token_address and PT3.pool_address <> PT1.pool_address
                join "Pool_Token" PT4 on PT3.pool_address = PT4.pool_address and PT3.token_address <> PT4.token_address
                join "Pool" P1 on PT1.pool_address = P1.pool_address
                join "Pool" P2 on PT3.pool_address = P2.pool_address
                where PT1.token_address = '{token1}' and PT4.token_address = '{token2}'
            """
        command_depth_3 = f"""
                select PT1.token_address as token1, PT2.pool_address as p1, P1.tvl as p1_tvl, 
                PT2.token_address as temp1, PT3.pool_address as p2, P2.tvl as p2_tvl, 
                PT4.token_address as temp2, PT5.pool_address as p3, P3.tvl as p3_tvl, 
                PT6.token_address as token2
                from "Pool_Token" PT1
                join "Pool_Token" PT2 on PT1.pool_address = PT2.pool_address and PT1.token_address <> PT2.token_address
                join "Pool_Token" PT3 on PT2.token_address = PT3.token_address and PT3.pool_address <> PT1.pool_address
                join "Pool_Token" PT4 on PT3.pool_address = PT4.pool_address and PT3.token_address <> PT4.token_address and PT4.token_address <> PT2.token_address
                join "Pool_Token" PT5 on PT4.token_address = PT5.token_address and PT5.pool_address <> PT3.pool_address and PT5.pool_address <> PT1.pool_address
                join "Pool_Token" PT6 on PT5.pool_address = PT6.pool_address and PT5.token_address <> PT6.token_address and PT6.token_address <> PT3.token_address
                join "Pool" P1 on PT1.pool_address = P1.pool_address
                join "Pool" P2 on PT3.pool_address = P2.pool_address
                join "Pool" P3 on PT5.pool_address = P3.pool_address
                where PT1.token_address = '{token1}' and PT6.token_address = '{token2}'
            """
        cursor.execute(command_depth_1)
        results = cursor.fetchall()
        count = 0
        for result in results:
            if result[2] >= 1000:
                print(result)
                count += 1
        print(count, " of ", len(results), " routes is valid in depth 1")
        cursor.execute(command_depth_2)
        results = cursor.fetchall()
        count = 0
        for result in results:
            if result[2] >= 1000 and result[5] >= 1000:
                count += 1
                print(result)
        print(count, " of ", len(results), " routes is valid in depth 2")
        cursor.execute(command_depth_3)
        results = cursor.fetchall()
        count = 0
        for result in results:
            if result[2] >= 1000 and result[5] >= 1000 and result[8] >= 1000:
                count += 1
                print(result)
        print(count, " of ", len(results), " routes is valid in depth 3")


def main():
    # get_row_data_main()
    start = datetime.now()
    init_db_main()
    # update_db_main()
    # get_route_data_main()
    print(datetime.now() - start)
    print("Test End")


# -- select *
# -- from "Pool_Token" PT1
# -- join "Pool_Token" PT2 on PT1.pool_address = PT2.pool_address
# -- where PT1.token_address = '0x2791bca1f2de4661ed88a30c99a7a9449aa84174' and PT2.token_address = '0x9cd6746665d9557e1b9a775819625711d0693439'

# -- select PT1.pool_address as PA1, P1.tvl as P1l, PT1.token_address as PT1,
# -- PT2.pool_address as PA2, PT2.token_address as PT2,
# -- PT3.pool_address as PA3, P2.tvl as P2l, PT3.token_address as PT3,
# -- PT4.pool_address as PA4, PT4.token_address as PT4
# -- from "Pool_Token" PT1
# -- join "Pool_Token" PT2 on PT1.pool_address = PT2.pool_address and PT1.token_address <> PT2.token_address
# -- join "Pool_Token" PT3 on PT2.token_address = PT3.token_address and PT3.pool_address <> PT1.pool_address
# -- join "Pool_Token" PT4 on PT3.pool_address = PT4.pool_address and PT3.token_address <> PT4.token_address
# -- join "Pool" P1 on PT1.pool_address = P1.pool_address
# -- join "Pool" P2 on PT3.pool_address = P2.pool_address
# -- where PT1.token_address = '0x2791bca1f2de4661ed88a30c99a7a9449aa84174' and PT4.token_address = '0x9cd6746665d9557e1b9a775819625711d0693439'


if __name__ == '__main__':
    main()
