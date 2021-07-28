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
job.commit()

## @type: DataSource
## @args: [database = "glue_db", table_name = "redshift_db_sales_stg_products", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: []
datasource_products = glueContext.create_dynamic_frame.from_catalog(database = "glue_db", table_name = "redshift_db_sales_stg_products", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasource_products")
## @type: DataSource
## @args: [database = "glue_db", table_name = "redshift_db_sales_tgt_dim_category", redshift_tmp_dir = args["TempDir"], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: []
datasource_category = glueContext.create_dynamic_frame.from_catalog(database = "glue_db", table_name = "redshift_db_sales_tgt_dim_category", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasource_category")
## @type: Join
## @args: [keys1 = [<keys1>], keys2 = [<keys2>]]
## @return: <output>
## @inputs: [frame1 = <frame1>, frame2 = <frame2>]
join_1 = Join.apply(frame1 = datasource_products, frame2 = datasource_category, keys1 = "category", keys2 = "category_id", transformation_ctx = "join_1")
## @type: SelectFields
## @args: [paths = ["paths"], transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: [frame = <frame>]
selectFields_2 = SelectFields.apply(frame = join_1, paths = ["product_id","product","unit_price","stock_code","category","categorysurrkey"], transformation_ctx = "selectFields_2")
## @type: ApplyMapping
## @args: [mappings = <mappings>, transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: [frame = <frame>]
applyMapping_3 = ApplyMapping.apply(frame = selectFields_2, mappings = [
    ("product_id","int","product_id","int"),
    ("product","string","product","string"),
    ("unit_price","double","unit_price","double"),
    ("stock_code","string","stock_code","string"),
    ("category","int","category","int"),
    ("categorysurrkey","int","dim_category","int"),
    ], transformation_ctx = "applyMapping_3")
    
selectfields_4 = SelectFields.apply(frame = applyMapping_3, paths = ["product", "product_id", "unit_price", "category", "stock_code","dim_category"], transformation_ctx = "selectfields_4")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "glue_db", table_name = "redshift_db_sales_stg_products", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice_5 = ResolveChoice.apply(frame = selectfields_4, choice = "MATCH_CATALOG", database = "glue_db", table_name = "redshift_db_sales_tgt_dim_products", transformation_ctx = "resolvechoice_5")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice_6 = ResolveChoice.apply(frame = resolvechoice_5, choice = "make_cols", transformation_ctx = "resolvechoice_6")

job.commit()





