############ REQUIREMENTS ####################
# sudo apt-get install python-pip 
# sudo apt-get install libpq-dev
# sudo pip install psycopg2
# sudo pip install sqlalchemy
# sudo pip install sqlalchemy-redshift
##############################################

import numpy as np
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('postgresql://awsuser:Sysco123@redshift-db.c9adkqwvnbdx.ap-southeast-1.redshift.amazonaws.com:5439/rs_source')
data_frame = pd.read_sql('SELECT * FROM public.customer;', engine)

print(data_frame)

# import sqlalchemy as sa
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy import create_engine

# #>>>>>>>> MAKE CHANGES HERE <<<<<<<<<<<<< 
# DATABASE = "rs_source"
# USER = "awsuser"
# PASSWORD = "Sysco123"
# HOST = "redshift-db.c9adkqwvnbdx.ap-southeast-1.redshift.amazonaws.com"
# PORT = "5439"
# SCHEMA = "public"      #default is "public" 

# ####### connection and session creation ############## 
# connection_string = "redshift+psycopg2://%s:%s@%s:%s/%s" % (USER,PASSWORD,HOST,str(PORT),DATABASE)
# engine = create_engine(connection_string)
# session = sessionmaker()
# session.configure(bind=engine)
# s = session()
# SetPath = "SET search_path TO %s" % SCHEMA
# s.execute(SetPath)
# ###### All Set Session created using provided schema  #######

# ################ write queries from here ###################### 
# query = "unload('select * from customer') to 's3://seed-data-lake/unload/customer_' iam_role 'arn:aws:iam::340246275766:role/Ec2';"
# rr = s.execute(query)
# all_results =  rr.fetchall()

# def pretty(all_results):
#     for row in all_results :
#         print("row start >>>>>>>>>>>>>>>>>>>>")
#         for r in row :
#             print(" ----" + r)
#         print ("row end >>>>>>>>>>>>>>>>>>>>>>")


# pretty(all_results)


# ########## close session in the end ###############
# s.close()