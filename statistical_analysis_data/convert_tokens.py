"""
Author: Justin Jia
Last Updated: August 21, 2023
Version: 1.0.1
"""

import json

# Load the data from the 'pool_8.18.json' file
with open('pool_8.18.json') as f:
    data = json.load(f)

result = []

# Iterate through each item in the data
for item in data:
    new_item = {
        'Name': item['Name'],
        'pool_address': item['pool_address'],
        'tvl': item['tvl'],
        'fee': item['fee']
    }

    # Iterate through tokens from 'token1' to 'token8'
    for i in range(1, 9):
        token = 'token' + str(i)
        if item[token]:
            # Add token information to the new_item dictionary
            new_item[token] = item[token]
            new_item[token + '_symbol'] = ''  # Placeholder for symbol
            new_item[token + '_decimals'] = '0'  # Placeholder for decimals

    result.append(new_item)

# Save the result to the 'output.json' file
with open('output.json', 'w') as f:
    json.dump(result, f)
