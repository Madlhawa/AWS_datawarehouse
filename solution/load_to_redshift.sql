truncate table redshift_db.rs_source.customer;
truncate table redshift_db.rs_source.orders;
truncate table redshift_db.rs_source.productCategories;
truncate table redshift_db.rs_source.products;

copy redshift_db.rs_source.customer
from 's3://seed-source-data/customers.csv'
iam_role 'arn:aws:iam::340246275766:role/Redshift'
DELIMITER ',' 
IGNOREHEADER 1 
TIMEFORMAT 'auto';

copy redshift_db.rs_source.orders
from 's3://seed-source-data/orders.csv'
iam_role 'arn:aws:iam::340246275766:role/Redshift'
DELIMITER ',' 
IGNOREHEADER 1 
TIMEFORMAT 'auto';

copy redshift_db.rs_source.productcategories
from 's3://seed-source-data/productCategories.csv'
iam_role 'arn:aws:iam::340246275766:role/Redshift'
DELIMITER ',' 
IGNOREHEADER 1 
TIMEFORMAT 'auto';

copy redshift_db.rs_source.products
from 's3://seed-source-data/products.csv'
iam_role 'arn:aws:iam::340246275766:role/Redshift'
DELIMITER ',' 
IGNOREHEADER 1 
TIMEFORMAT 'auto';