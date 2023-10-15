import json

with open('include/test.json', 'r') as file:
            data = json.load(file)

desired_keys = ['id', 'site_id', 'title', 'price', 'sold_quantity', 'thumbnail']
list_of_values = [[d[key] for key in desired_keys] for d in data]
list_of_tuples = [tuple(value,) for value in list_of_values]

