import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "glue_db", table_name = "products000", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "glue_db", table_name = "products000", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("product_id", "int", "product_id", "int"), ("product", "string", "product", "string"), ("unit_price", "double", "unit_price", "double"), ("stock_code", "string", "stock_code", "string"), ("category", "int", "category", "int")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("product_id", "int", "product_id", "int"), ("product", "string", "product", "string"), ("unit_price", "double", "unit_price", "double"), ("stock_code", "string", "stock_code", "string"), ("category", "int", "category", "int")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["product", "product_id", "unit_price", "category", "stock_code"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["product", "product_id", "unit_price", "category", "stock_code"], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "glue_db", table_name = "redshift_db_sales_stg_products", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "glue_db", table_name = "redshift_db_sales_stg_products", transformation_ctx = "resolvechoice3")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")
## @type: DataSink
## @args: [database = "glue_db", table_name = "redshift_db_sales_stg_products", redshift_tmp_dir = TempDir, transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
# datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "glue_db", table_name = "redshift_db_sales_stg_products", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink5")
datasink_6 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = resolvechoice4,catalog_connection = "redshift", connection_options = { "preactions":"truncate table sales.stg_products;", "dbtable": "sales.stg_products", "database": "redshift_db"},redshift_tmp_dir = args["TempDir"])

job.commit()

product = glueContext.create_dynamic_frame.from_catalog(database = "glue_db", table_name = "redshift_db_sales_stg_products", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasource0")## @type: DataSource

category = glueContext.create_dynamic_frame.from_catalog(database = "glue_db", table_name = "redshift_db_sales_tgt_dim_category", redshift_tmp_dir = args["TempDir"], transformation_ctx = "category")

join = Join.apply(frame1 = product, frame2 = category, keys1 = ['category'], keys2 = ['category_id'], transformation_ctx = "join")
applymappingproduct = ApplyMapping.apply(frame = join, mappings = [("product", "string", "product", "string"), ("product_id", "int", "product_id", "int"), ("unit_price", "double", "unit_price", "double"), ("category", "int", "category", "int"), ("stock_code", "string", "stock_code", "string"),("categorysurrkey", "int", "dim_category", "int"),], transformation_ctx = "applymapping1")


selectfields2 = SelectFields.apply(frame = applymappingproduct, paths = ["product", "product_id", "dim_category", "unit_price", "category", "stock_code"], transformation_ctx = "selectfields2")

resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "glue_db", table_name = "redshift_db_sales_tgt_dim_products", transformation_ctx = "resolvechoice3")

resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

# datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "glue_db", table_name = "redshift_db_sales_tgt_dim_products", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink5")
datasink_6 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = resolvechoice4,catalog_connection = "redshift", connection_options = { "preactions":"truncate table sales.tgt_dim_products;", "dbtable": "sales.tgt_dim_products", "database": "redshift_db"},redshift_tmp_dir = args["TempDir"])

job.commit()