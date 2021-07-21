import boto3
import pandas

client = boto3.client(
    's3',
    aws_access_key_id = 'AKIAU6OCCE23FELOD3WN',
    aws_secret_access_key = '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',
    region_name = 'ap-southeast-1'
)
    
resource = boto3.resource(
    's3',
    aws_access_key_id = 'AKIAU6OCCE23FELOD3WN',
    aws_secret_access_key = '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',
    region_name = 'ap-southeast-1'
)

clientResponse = client.list_buckets()

print('Printing bucket names...')
for bucket in clientResponse['Buckets']:
    print(f'Bucket Name: {bucket["Name"]}')

obj = client.get_object(
    Bucket = 'seed-source-data',
    Key = 'customers.csv'
)

data = pandas.read_csv(obj['Body'])

# Print the data frame
print('Printing the data frame...')
print(data)