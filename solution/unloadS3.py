from sqlalchemy import create_engine
from configparser import ConfigParser

config = ConfigParser()
config.read('config.ini')

engine = create_engine('postgresql://'+config['redshift']['username']+':'+config['redshift']['password']+'@redshift-cluster.c9adkqwvnbdx.ap-southeast-1.redshift.amazonaws.com:5439/redshift_db')

tables = [
    'customer',
    'orders',
    'products',
    'productcategories'
]
with engine.connect() as connection:
    for table in tables:
        query = f"unload('select * from rs_source.{table}') to 's3://seed-data-lake/{table}' iam_role 'arn:aws:iam::340246275766:role/Redshift' parallel off allowoverwrite csv;"
        result = connection.execute(query)
        print(f'loding table {table}')

print(result)