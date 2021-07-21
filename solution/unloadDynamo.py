import boto3
import json
import pandas as pd

dynamodb = boto3.resource('dynamodb',
    region_name='ap-southeast-1'
)
table = dynamodb.Table('Address')

response = table.scan(
    Select="ALL_ATTRIBUTES",
    )

response = json.dumps(response)
address_json = json.loads(response)
address_dataframe = pd.json_normalize(address_json['Items'], max_level=1)

address1_dataframe = address_dataframe.drop('Address.AddressLine2', axis=1).rename({'Address.AddressLine1':'Address'},axis=1)
address2_dataframe = address_dataframe.drop('Address.AddressLine1', axis=1).rename({'Address.AddressLine2':'Address'},axis=1)

address_dataframe = address1_dataframe.append(address2_dataframe, ignore_index=True)

address_dataframe = address_dataframe.join(address_dataframe['Address'].apply(json.dumps).apply(json.loads).apply(pd.Series)).drop('Address', axis=1)
address_dataframe = address_dataframe[address_dataframe.eval('Street != "NULL" & PostalCode != "NULL" & City != "NULL" & StateProvinceName != "NULL"')]
print(address_dataframe)