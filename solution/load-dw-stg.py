from sqlalchemy import create_engine

engine = create_engine('postgresql://awsuser:Sysco123@redshift-cluster.c9adkqwvnbdx.ap-southeast-1.redshift.amazonaws.com:5439/redshift_db')


tables = [
    'customer',
    'address',
    'orders'
]

with engine.connect() as connection:
    for table in tables:
        query = f"truncate table redshift_db.sales.stg_{table}; copy redshift_db.sales.stg_{table} from 's3://seed-data-lake/{table+'000' if table != 'address' else 'address.csv'}' iam_role 'arn:aws:iam::340246275766:role/Redshift' DELIMITER ',' TIMEFORMAT 'auto';"
        result = connection.execute(query)
        print(f'loding table {table}')