import psycopg2
import numpy as np
from sqlalchemy import create_engine
import pandas as pd

# con=psycopg2.connect(dbname= 'rs_source',
#                     host='redshift-db.c9adkqwvnbdx.ap-southeast-1.redshift.amazonaws.com', 
#                     port= '5439', 
#                     user= 'awsuser', 
#                     password= 'Sysco123')

# cur = con.cursor()
# cur.execute("SELECT * FROM `customer`;")
# data = np.array(cur.fetchall())

engine = create_engine('postgresql://awsuser:Sysco123@redshift-db.c9adkqwvnbdx.ap-southeast-1.redshift.amazonaws.com:5439/rs_source')
data_frame = pd.read_sql('SELECT * FROM `public.customer`;', engine)

print(data_frame)

# cur.close() 
# con.close()