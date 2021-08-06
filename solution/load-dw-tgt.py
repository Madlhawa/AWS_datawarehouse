from sqlalchemy import create_engine
from configparser import ConfigParser

config = ConfigParser()
config.read('config.ini')

engine = create_engine('postgresql://'+config['redshift']['username']+':'+config['redshift']['password']+'@redshift-cluster.c9adkqwvnbdx.ap-southeast-1.redshift.amazonaws.com:5439/redshift_db')
with open('sql_scripts/load-dw-tgt.sql') as f:
    query = f.read()
with engine.connect() as connection:
    result = connection.execute(query)

print("data loaded into target")