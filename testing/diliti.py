import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SQLContext

args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
sqlContext =SQLContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

productcsv = glueContext.create_dynamic_frame.from_catalog(database = "dev", table_name = "products_csv", transformation_ctx = "productcsv")
category = glueContext.create_dynamic_frame_from_options(connection_type = "redshift", connection_options = {"url": "jdbc:redshift://redshift-cluster-1.ca7n2z5hykdh.us-east-1.redshift.amazonaws.com:5439/dev", "user": "awsuser", "password": "ItDilini4","dbtable": "sales.category", "redshiftTmpDir": "s3://test-sampledata/"} )
applymapping1 = ApplyMapping.apply(frame = productcsv, mappings = [("product_id", "long", "p_productid", "int"), ("product", "string", "p_pname", "string"), ("unit_price", "double", "p_unitprice", "decimal(10,2)"), ("stock_code", "string", "p_stockcode", "string"), ("category", "long", "p_category", "int")], transformation_ctx = "applymapping1")
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["p_unitprice", "p_category", "p_productid", "p_pname", "p_stockcode"], transformation_ctx = "selectfields2")
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "dev", table_name = "dev_sales_stg_products", transformation_ctx = "resolvechoice3")
resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")
datasink5=glueContext.write_dynamic_frame.from_jdbc_conf(frame = resolvechoice4,catalog_connection = "redshift", connection_options = { "preactions":"truncate table sales.stg_products;", "dbtable": "sales.stg_products", "database": "dev"},redshift_tmp_dir = args["TempDir"])

job.commit()

## getting stg_products table data from RS
stg = glueContext.create_dynamic_frame_from_options(connection_type = "redshift", connection_options = {"url": "jdbc:redshift://redshift-cluster-1.ca7n2z5hykdh.us-east-1.redshift.amazonaws.com:5439/dev", "user": "awsuser", "password": "ItDilini4","dbtable": "sales.stg_products", "redshiftTmpDir": "s3://test-sampledata/"} )
## Apply transformation, join the tables
join1 = Join.apply(frame1 = stg, frame2 = category, keys1 = "p_category", keys2 = "c_categoryid", transformation_ctx = "join1")
##select the columns needed
select1=SelectFields.apply(frame = join1, paths = ["p_productid", "p_pname", "p_stockcode", "p_unitprice", "c_cat_skey" ], transformation_ctx = "select1")
###map the columns
applymapping1 = ApplyMapping.apply(frame = select1, mappings = [("p_productid", "int", "p_productid", "int"), ("p_pname", "string", "p_pname", "string"),("p_stockcode", "string", "p_stockcode", "string"),("p_unitprice", "decimal(10,2)", "p_unitprice", "decimal(10,2)"),("c_cat_skey", "int", "p_category", "int")], transformation_ctx = "applymapping1")
#select fields - adding product skey
select2=SelectFields.apply(frame = applymapping1, paths = ["p_productid", "p_pname", "p_stockcode", "p_unitprice","p_category" ], transformation_ctx = "select2")
datasink1=glueContext.write_dynamic_frame.from_jdbc_conf(frame = select2,catalog_connection = "redshift", connection_options = { "preactions":"truncate table sales.products;", "dbtable": "sales.products", "database": "dev"},redshift_tmp_dir = args["TempDir"])
job.commit()