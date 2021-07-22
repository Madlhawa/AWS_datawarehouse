CREATE SCHEMA Sales authorization awsuser;

CREATE TABLE Sales.stg_customer (
    CustomerID  int NOT NULL,
    FirstName   nvarchar(50),
    MiddleName  nvarchar(50),
    LastName    nvarchar(50),
    Gender      nvarchar(50),
    PhoneNumber nvarchar(50),
    PhoneNumberType nvarchar(50),
    EmailAddress    nvarchar(100),
    PRIMARY KEY (CustomerID)
);

CREATE TABLE Sales.stg_orders (
    Order_id    INT NOT NULL,
    Order_date  nvarchar(50),
    Order_status    nvarchar(50),
    Product_id  INT,
    Qty_ordered INT,
    Unit_price  FLOAT,
    Subtotal    FLOAT,
    Customer_id INT,
    PRIMARY KEY (Order_id)
);

CREATE TABLE Sales.stg_category (
    Category_ID INT NOT NULL,
    Category    nvarchar(50),
    PRIMARY KEY (Category_ID)
);

CREATE TABLE Sales.stg_products (
    Product_id  INT NOT NULL,
    Product     nvarchar(50),
    unit_price  FLOAT,
    Stock_code  nvarchar(50),
    Category    INT,
    PRIMARY KEY (Product_id)
);

CREATE TABLE Sales.stg_address (
    CustomerID  INT NOT NULL,
    Country     nvarchar(50),
    AddressType  nvarchar(50),
    Street  nvarchar(50),
    PostalCode    nvarchar(50),
    City nvarchar(50),
    StateProviceName nvarchar(50)
);