"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import json
import pandas as pd

# Load the JSON data from the 'bsc_pool.json' file
with open('bsc_pool.json', 'r') as file:
    bsc_pool = json.load(file)

data = bsc_pool

# Create a dictionary to store statistics about pool names
name_stats = {}

# Iterate through each entry in the data
for entry in data:
    name = entry["Name"]
    tvl = entry["tvl"]

    # Initialize name statistics if it doesn't exist in the dictionary
    if name not in name_stats:
        name_stats[name] = {"total_count": 0, "zero_tvl_count": 0}

    # Increment the total count for the name
    name_stats[name]["total_count"] += 1
    # If the TVL is zero, increment the zero TVL count
    if tvl == 0:
        name_stats[name]["zero_tvl_count"] += 1

# Convert the name_stats dictionary to a DataFrame
df = pd.DataFrame.from_dict(name_stats, orient='index').reset_index()

# Calculate the valid TVL count for each name
df["Valid TVL Count"] = df["total_count"] - df["zero_tvl_count"]

# Rename DataFrame columns
df.columns = ["Name", "Total Count", "Zero TVL Count", "Valid TVL Count"]

# Save the DataFrame to a CSV file
df.to_csv("tvl_stats.csv", index=False)

print("Data has been successfully saved to tvl_stats.csv!")

