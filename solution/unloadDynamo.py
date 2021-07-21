import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Address')

response = table.scan(
    Select="ALL_ATTRIBUTES",
    )

print(response)