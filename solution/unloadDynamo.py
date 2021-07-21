import boto3

dynamodb = boto3.resource('dynamodb',
    region_name='ap-southeast-1'
)
table = dynamodb.Table('Address')

response = table.scan(
    Select="ALL_ATTRIBUTES",
    )

print(response)