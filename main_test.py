"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

from main import init_db_main, update_db_main, get_route_data_main, get_row_data_main
from datetime import datetime


def main():
    # get_row_data_main()
    start = datetime.now()
    # init_db_main()
    # update_db_main()
    get_route_data_main()
    print(datetime.now() - start)
    print("Test End")


if __name__ == '__main__':
    main()
