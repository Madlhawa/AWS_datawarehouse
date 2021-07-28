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

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "glue_db", table_name = "productcategories000", transformation_ctx = "datasource0")
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("category_id", "long", "category_id", "int"), ("category", "string", "category", "string")], transformation_ctx = "applymapping1")
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["category_id", "category"], transformation_ctx = "selectfields2")
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "glue_db", table_name = "redshift_db_sales_stg_category", transformation_ctx = "resolvechoice3")
resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "glue_db", table_name = "redshift_db_sales_stg_category", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink5")
job.commit()

stg = glueContext.create_dynamic_frame_from_options(connection_type = "redshift", connection_options = {"url": "jdbc:redshift://redshift-cluster.c9adkqwvnbdx.ap-southeast-1.redshift.amazonaws.com:5439/redshift_db", "user": "awsuser", "password": "Sysco123","dbtable": "sales.stg_category", "redshiftTmpDir": args["TempDir"]} )
applymapping2 = ApplyMapping.apply(frame = stg, mappings = [("category_id", "int", "category_id", "int"), ("category", "string", "category", "string")], transformation_ctx = "applymapping2")
datasink2=glueContext.write_dynamic_frame.from_jdbc_conf(frame = applymapping2,catalog_connection = "redshift", connection_options = { "preactions":"truncate table Sales.Tgt_Dim_category;", "dbtable": "Sales.Tgt_Dim_category", "database": "redshift_db"},redshift_tmp_dir = args["TempDir"])
job.commit()