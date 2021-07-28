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
## @args: [database = "glue_db", table_name = "productcategories000", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "glue_db", table_name = "productcategories000", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("category_id", "long", "category_id", "int"), ("category", "string", "category", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("category_id", "long", "category_id", "int"), ("category", "string", "category", "string")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["category_id", "category"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["category_id", "category"], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "glue_db", table_name = "redshift_db_sales_stg_category", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "glue_db", table_name = "redshift_db_sales_stg_category", transformation_ctx = "resolvechoice3")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

# datasink6 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = resolvechoice4,catalog_connection = "redshift", connection_options = { "preactions":"truncate table sales.stg_category;", "dbtable": "sales.stg_category", "database": "redshift_db"},redshift_tmp_dir = args["TempDir"])
## @type: DataSink
## @args: [database = "glue_db", table_name = "redshift_db_sales_stg_category", redshift_tmp_dir = TempDir, transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
#datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "glue_db", table_name = "redshift_db_sales_stg_category", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink5")

## @type: DataSource
## @args: [database = "glue_db", table_name = "redshift_db_sales_stg_category", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: []
datasource_1 = glueContext.create_dynamic_frame.from_catalog(database = "glue_db", table_name = "redshift_db_sales_stg_category", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasource_1")
## @type: ApplyMapping
## @args: [mappings = <mappings>, transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: [frame = <frame>]
applymapping_2 = ApplyMapping.apply(frame = datasource_1, mappings = [("category_id", "int", "category_id", "int"), ("category", "string", "category", "string")], transformation_ctx = "applymapping_2")
## @type: SelectFields
## @args: [paths = ["paths"], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: [frame = <frame>]
selectfields_3 = SelectFields.apply(frame = applymapping_2, paths = ["category_id", "category"], transformation_ctx = "selectfields_3")
resolvechoice_4 = ResolveChoice.apply(frame = selectfields_3, choice = "MATCH_CATALOG", database = "glue_db", table_name = "redshift_db_sales_tgt_dim_category", transformation_ctx = "resolvechoice_4")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice_5 = ResolveChoice.apply(frame = resolvechoice_4, choice = "make_cols", transformation_ctx = "resolvechoice_5")

datasink_6 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = resolvechoice4,catalog_connection = "redshift", connection_options = { "preactions":"truncate table sales.tgt_dim_category;", "dbtable": "sales.tgt_dim_category", "database": "redshift_db"},redshift_tmp_dir = args["TempDir"])

job.commit()