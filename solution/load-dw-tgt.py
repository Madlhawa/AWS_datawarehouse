from sqlalchemy import create_engine

engine = create_engine('postgresql://awsuser:Sysco123@redshift-cluster.c9adkqwvnbdx.ap-southeast-1.redshift.amazonaws.com:5439/redshift_db')
query = """
TRUNCATE TABLE Sales.Tgt_Dim_customer;
TRUNCATE TABLE Sales.Tgt_Fact_orders;
TRUNCATE TABLE Sales.Tgt_Dim_address;

INSERT INTO Sales.Tgt_Dim_customer (CustomerID, FirstName, MiddleName, LastName, Gender, PhoneNumber, PhoneNumberType, EmailAddress)
SELECT CustomerID, FirstName, MiddleName, LastName, Gender, PhoneNumber, PhoneNumberType, EmailAddress
FROM Sales.stg_customer;

INSERT INTO Sales.Tgt_Fact_orders (Order_id, Order_date, Order_status, Product_id, Qty_ordered, Unit_price, Subtotal, Customer_id, Dim_Customer, Dim_Product)
SELECT o.Order_id, o.Order_date, o.Order_status, o.Product_id, o.Qty_ordered, o.Unit_price, o.Subtotal, o.Customer_id, c.customersurrkey AS Dim_Customer, p.productsurrkey AS Dim_Product
FROM Sales.stg_orders o
LEFT OUTER JOIN Sales.Tgt_Dim_customer c ON o.Customer_id = c.CustomerID
LEFT OUTER JOIN Sales.Tgt_Dim_products p ON o.Product_id = p.product_id;

INSERT INTO Sales.Tgt_Dim_address (customerid, dim_customer, country, addresstype, street, postalcode, city, stateprovicename)
SELECT a.customerid, c.customersurrkey AS dim_customer, a.country, a.addresstype, a.street, a.postalcode, a.city, a.stateprovicename
FROM Sales.stg_address a
LEFT OUTER JOIN Sales.Tgt_Dim_customer c ON a.customerid = c.CustomerID;
"""
with engine.connect() as connection:
    result = connection.execute(query)

print("data loaded into target")