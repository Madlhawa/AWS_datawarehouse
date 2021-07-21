truncate table rs_source.public.customer;
truncate table rs_source.public.orders;
truncate table rs_source.public.productCategories;
truncate table rs_source.public.products;

--copy customer from 's3://seed-source-data/customers.csv' access_key_id 'AKIAU6OCCE23FELOD3WN' secret_access_key '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' DELIMITER ',' IGNOREHEADER 1 TIMEFORMAT 'auto';

copy rs_source.public.customer
from 's3://seed-source-data/customers.csv'
iam_role 'arn:aws:iam::340246275766:role/Redshift'
DELIMITER ',' 
IGNOREHEADER 1 
TIMEFORMAT 'auto';

copy rs_source.public.orders
from 's3://seed-source-data/orders.csv'
iam_role 'arn:aws:iam::340246275766:role/Redshift'
DELIMITER ',' 
IGNOREHEADER 1 
TIMEFORMAT 'auto';

copy rs_source.public.productcategories
from 's3://seed-source-data/productCategories.csv'
iam_role 'arn:aws:iam::340246275766:role/Redshift'
DELIMITER ',' 
IGNOREHEADER 1 
TIMEFORMAT 'auto';

copy rs_source.public.products
from 's3://seed-source-data/products.csv'
iam_role 'arn:aws:iam::340246275766:role/Redshift'
DELIMITER ',' 
IGNOREHEADER 1 
TIMEFORMAT 'auto';

truncate table rs_source.public.customer;
truncate table rs_source.public.orders;
truncate table rs_source.public.productCategories;
truncate table rs_source.public.products;

--copy customer from 's3://seed-source-data/customers.csv' access_key_id 'AKIAU6OCCE23FELOD3WN' secret_access_key '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' DELIMITER ',' IGNOREHEADER 1 TIMEFORMAT 'auto';

copy rs_source.public.customer
from 's3://seed-source-data/customers.csv'
iam_role 'arn:aws:iam::340246275766:role/Redshift'
DELIMITER ',' 
IGNOREHEADER 1 
TIMEFORMAT 'auto';

copy rs_source.public.orders
from 's3://seed-source-data/orders.csv'
iam_role 'arn:aws:iam::340246275766:role/Redshift'
DELIMITER ',' 
IGNOREHEADER 1 
TIMEFORMAT 'auto';

copy rs_source.public.productcategories
from 's3://seed-source-data/productCategories.csv'
iam_role 'arn:aws:iam::340246275766:role/Redshift'
DELIMITER ',' 
IGNOREHEADER 1 
TIMEFORMAT 'auto';

copy rs_source.public.products
from 's3://seed-source-data/products.csv'
iam_role 'arn:aws:iam::340246275766:role/Redshift'
DELIMITER ',' 
IGNOREHEADER 1 
TIMEFORMAT 'auto';