import numpy as np
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

engine = create_engine('postgresql://awsuser:Sysco123@redshift-db.c9adkqwvnbdx.ap-southeast-1.redshift.amazonaws.com:5439/rs_source')

tables = [
    'customer',
    'orders',
    'products',
    'productcategories'
]
with engine.connect() as connection:
    for table in tables:
        query = f"unload('select * from {table}') to 's3://seed-data-lake/{table}/{str(datetime.now()).replace(' ','_')}/{table}_' iam_role 'arn:aws:iam::340246275766:role/Redshift'csv;"
        result = connection.execute()

print(result)