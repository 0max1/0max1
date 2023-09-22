"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""


# Function to update the pair_flag in the database
# maybe need another find_route_pair_ids to update the pairs which flag is false in the last update
# Actually, what does pair_id mean. Ask this in the meeting and I need answer in Chinese
def update_pair_flag(connection, filename):
    # Check if database connection is available
    if connection is None:
        print("Database connection is not available.")
        return

    try:

        # Open database connection
        with connection:

            # Open file containing pair IDs
            with open(filename, 'r') as file:
                # Iterate through each line in file
                for line in file:
                    # Get the pair ID
                    pair_id = int(line.strip())

                    # Construct UPDATE query
                    update_query = f'UPDATE "Pair" SET pair_flag = false WHERE pair_id = {pair_id}'

                    # Execute query using cursor
                    with connection.cursor() as cursor:
                        # Execute the update query
                        cursor.execute(update_query)

                        # Print confirmation
                        # print(f"Updated pair_flag for pair_id {pair_id}")

    # Handle any exceptions
    except Exception as e:
        print(f"An error occurred: {e}")

    # Close database connection
    connection.close()
