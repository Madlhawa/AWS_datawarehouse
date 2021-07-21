import numpy as np
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('postgresql://awsuser:Sysco123@redshift-db.c9adkqwvnbdx.ap-southeast-1.redshift.amazonaws.com:5439/rs_source')
data_frame = pd.read_sql('SELECT * FROM public.customer;', engine)

print(data_frame)