TEST_DB.TEST_DB_SCHEMA.TESTSTAGE--- create database

create database test_db;

use test_db;

-- create schema

create schema test_db_Schema;

-- create tables

-- Dimension Table: DimDate
CREATE TABLE DimDate (
    DateID INT PRIMARY KEY,
    Date DATE,
    DayOfWeek VARCHAR(10),
    Month VARCHAR(10),
    Quarter INT,
    Year INT,
    IsWeekend BOOLEAN
);

-- Dimension Table: DimLoyaltyProgram
CREATE TABLE DimLoyaltyProgram (
    LoyaltyProgramID INT PRIMARY KEY,
    ProgramName VARCHAR(100),
    ProgramTier VARCHAR(50),
    PointsAccrued INT
);

-- Dimension Table: DimCustomer
-- Drop old table
DROP TABLE IF EXISTS DIMCUSTOMER;
CREATE TABLE DimCustomer (
    CustomerID INT PRIMARY KEY autoincrement start 1 increment 1,
    FirstName VARCHAR(100),
    LastName VARCHAR(100),
    Gender VARCHAR(50),
    DateOfBirth DATE,
    Email VARCHAR(100),
    PhoneNumber VARCHAR(300),
    Address VARCHAR(500),
    City VARCHAR(100),
    State VARCHAR(100),
    PostalCode VARCHAR(100),
    Country VARCHAR(100),
    LoyaltyProgramID INT
);


-- Dimension Table: DimProduct
CREATE TABLE DimProduct (
    ProductID INT PRIMARY KEY autoincrement start 1 increment 1,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    Brand VARCHAR(50),
    UnitPrice DECIMAL(10, 2)
);


-- Dimension Table: DimStore
CREATE TABLE DimStore (
    StoreID INT PRIMARY KEY autoincrement start 1 increment 1,
    StoreName VARCHAR(100),
    StoreType VARCHAR(50),
	StoreOpeningDate DATE,
    Address VARCHAR(255),
    City VARCHAR(50),
    State VARCHAR(50),
    Region VARCHAR(50),
    ManagerName VARCHAR(100)
);


-- Fact Table: FactOrders
DROP TABLE IF EXISTS FactOrders;
CREATE TABLE FactOrders(
    OrderID INT PRIMARY KEY autoincrement start 1 increment 1,
    DateID INT,
    ProductID INT,
    StoreID INT,
    CustomerID INT,
    QuantityOrdered INT,
    OrderAmount DECIMAL(10, 2),
    DiscountAmount DECIMAL(10, 2),
    ShippingCost DECIMAL(10, 2),
    TotalAmount DECIMAL(10, 2),
    FOREIGN KEY (DateID) REFERENCES DimDate(DateID),
    FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID),
    FOREIGN KEY (ProductID) REFERENCES DimProduct(ProductID),
    FOREIGN KEY (StoreID) REFERENCES DimStore(StoreID)
);


-- creating file format
CREATE OR REPLACE FILE FORMAT CSV_SOURCE_FILE_FORMAT
TYPE = 'CSV'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
DATE_FORMAT = 'YYYY-MM-DD';


-- create stage 
-- a storage location where you can store data 
-- files before loading them into Snowflake tables 
-- or after unloading data from tables)

CREATE OR REPLACE STAGE TESTSTAGE;

-- run for local using snowsql

-- PUT local_file_path Stage TEST_DB.TEST_DB_SCHEMA.TESTSTAGE

PUT 'file://C:/Users/A.W/ACNDA/envs/myenv/AI with Python/LoyaltyProgram.csv' @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/DimLoyaltyInfo/AUTO_COMPRESS=FALSE;

PUT 'file://C:/Users/A.W/ACNDA/envs/myenv/AI with Python/DimCustommerData.csv' @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/DimCustomerData/AUTO_COMPRESS=FALSE;

PUT 'file://C:/Users/A.W/ACNDA/envs/myenv/AI with Python/DimProductData.csv' @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/DimProductData/AUTO_COMPRESS=FALSE;

PUT 'file://C:/Users/A.W/ACNDA/envs/myenv/AI with Python/DimDate.csv' @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/DimDate/AUTO_COMPRESS=FALSE;

PUT 'file://C:/Users/A.W/ACNDA/envs/myenv/AI with Python/DimStoreDataII.csv' @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/DimStoreDataII/AUTO_COMPRESS=FALSE;

PUT 'file://C:/Users/A.W/ACNDA/envs/myenv/AI with Python/FactOrdersII.csv' @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/FactOrdersII/AUTO_COMPRESS=FALSE;

PUT 'file://C:/Users/A.W/ACNDA/envs/myenv/AI with Python/StoreBurst/*.csv' @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/StoreBurst/AUTO_COMPRESS=FALSE;


-- LOAD DATA INTO OUR FILE

COPY INTO DIMLOYALTYPROGRAM
FROM @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/DimLoyaltyInfo/
FILE_FORMAT = (FORMAT_NAME = 'CSV_SOURCE_FILE_FORMAT')
ON_ERROR = 'CONTINUE';

SELECT * FROM DIMLOYALTYPROGRAM;

DESC TABLE DIMCUSTOMER;

ALTER TABLE DIMCUSTOMER
MODIFY COLUMN LastName VARCHAR(300);

ALTER TABLE DIMCUSTOMER
MODIFY COLUMN Gender VARCHAR(50);

ALTER TABLE DIMCUSTOMER MODIFY COLUMN ADDRESS VARCHAR(900);


SELECT 
    $1 AS Col1,
    $2 AS Col2,
    $3 AS Col3,
    $4 AS Col4,
    $5 AS Col5,
    $6 AS Col6,
    $7 AS Col7,
    $8 AS Col8,
    $9 AS Col9,
    $10 AS Col10,
    $11 AS Col11
FROM @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/DimCustomer/
(FILE_FORMAT => 'CSV_SOURCE_FILE_FORMAT')
LIMIT 5;

COPY INTO DIMCUSTOMER(FirstName, LastName, Gender, DateOfBirth, Email, PhoneNumber, Address, City, State, PostalCode, Country, LoyaltyProgramID)
FROM @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/DimCustomerData/
FILE_FORMAT = (FORMAT_NAME = 'CSV_SOURCE_FILE_FORMAT')
ON_ERROR = 'CONTINUE';

select * from dimcustomer;


COPY INTO DIMPRODUCT(ProductName, Category, Brand, UnitPrice)
FROM @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/DimProductData/
FILE_FORMAT = (FORMAT_NAME = 'CSV_SOURCE_FILE_FORMAT')
ON_ERROR = 'CONTINUE';

select * from DIMPRODUCT;

COPY INTO DIMDATE(DateID,Date,DayOfWeek,Month,Quarter,Year,IsWeekend)
FROM @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/DimDate/
FILE_FORMAT = (FORMAT_NAME = 'CSV_SOURCE_FILE_FORMAT')
ON_ERROR = 'CONTINUE';

select * from DIMDATE;


COPY INTO DIMSTORE(StoreName,StoreType,StoreOpeningDate,Address,City,State,Region,ManagerName)
FROM @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/DimStoreDataII/
FILE_FORMAT = (FORMAT_NAME = 'CSV_SOURCE_FILE_FORMAT')
ON_ERROR = 'CONTINUE';

select * from DIMSTORE;


DESC TABLE FACTORDERS;

COPY INTO FACTORDERS(DateID,ProductID,StoreID,CustomerID,QuantityOrdered,OrderAmount,DiscountAmount,ShippingCost,TotalAmount)
FROM @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/FactOrdersII/
FILE_FORMAT = (FORMAT_NAME = 'CSV_SOURCE_FILE_FORMAT')
ON_ERROR = 'CONTINUE';

select * from FACTORDERS LIMIT 100;

COPY INTO FACTORDERS(DateID,ProductID,StoreID,CustomerID,OrderAmount,DiscountAmount,ShippingCost,TotalAmount)
FROM @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/StoreBurst/
FILE_FORMAT = (FORMAT_NAME = 'CSV_SOURCE_FILE_FORMAT')
ON_ERROR = 'CONTINUE';

select * from FACTORDERS LIMIT 100;



LIST @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/DimLoyaltyInfo/;


-- create a new user 

CREATE OR REPLACE USER Test_PowerBI_User
    PASSWORD = 'Test_PowerBI_User'
    LOGIN_NAME = 'PowerBI User'
    DEFAULT_ROLE = 'ACCOUNTADMIN'
    DEFAULT_WAREHOUSE = 'COMPUTE_WH'
    MUST_CHANGE_PASSWORD = TRUE;
    

-- grant it account admin access

GRANT ROLE  ACCOUNTADMIN TO USER Test_PowerBI_User;

SHOW USERS LIKE '%POWERBI%';

DESC USER Test_PowerBI_User;

-- Drop existing user if any
DROP USER IF EXISTS Test_PowerBI_User;

-- Create new user (LOGIN_NAME defaults to username)
CREATE USER Test_PowerBI_User
    PASSWORD = 'Password123!'  -- Use strong password
    LOGIN_NAME = 'PowerBI_User'
    DEFAULT_ROLE = 'ACCOUNTADMIN'
    DEFAULT_WAREHOUSE = 'COMPUTE_WH'
    MUST_CHANGE_PASSWORD = TRUE
    COMMENT = 'Power BI integration user';

ALTER USER Test_PowerBI_User SET DISABLED = FALSE;
    
-- Grant ACCOUNTADMIN role
GRANT ROLE ACCOUNTADMIN TO USER Test_PowerBI_User;
-- use that user to load the data

-- Step 1: Create test user
DROP USER IF EXISTS Test_PowerBI_User;

CREATE USER TEST_LOGIN_USER
    PASSWORD = 'TestPass123!'
    DEFAULT_ROLE = 'PUBLIC'
    DEFAULT_WAREHOUSE = 'COMPUTE_WH'
    MUST_CHANGE_PASSWORD = FALSE;  -- For testing only

-- Step 2: Grant minimal access
GRANT ROLE PUBLIC TO USER TEST_LOGIN_USER;

-- Step 3: Verify
SHOW USERS LIKE 'TEST_LOGIN_USER';

-- Step 4: Try logging in with:
-- Username: TEST_LOGIN_USER
-- Password: TestPass123!
-- Account: ILMEJEY-DZC29393

-- Step 5: If successful, delete test user
-- DROP USER TEST_LOGIN_USER;

DROP USER IF EXISTS TEST_LOGIN_USER;

CREATE USER POWERBI_USER
    PASSWORD = 'YourSecurePassword123!'
    DEFAULT_ROLE = 'SYSADMIN'
    DEFAULT_WAREHOUSE = 'COMPUTE_WH'
    MUST_CHANGE_PASSWORD = TRUE;

GRANT ROLE SYSADMIN TO USER POWERBI_USER;