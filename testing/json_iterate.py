import json

with open('./dataset-sent/address.json', encoding='utf-8-sig') as file:
    json_file = json.load(file)

print(json_file['Addresses'][1])