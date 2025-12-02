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
CREATE TABLE FactOrders (
    OrderID INT PRIMARY KEY autoincrement start 1 increment 1,
    DateID INT,
    ProductID INT,
    StoreID INT,
    CustomerID INT,
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

PUT 'file://C:/Users/A.W/ACNDA/envs/myenv/AI with Python/factorders.csv' @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/factorders/AUTO_COMPRESS=FALSE;

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

LIST @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/DimLoyaltyInfo/;
-- create a new user 

-- grant it account admin access

-- use that user to load the data