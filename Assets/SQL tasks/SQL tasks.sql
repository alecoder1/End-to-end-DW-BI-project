--- 1. Update the store table  so that all stores have opening date
-- on or after 1-Jan-2014. Populate random values.

select * from dimstore;

UPDATE DIMSTORE SET STOREOPENINGDATE = DATEADD(DAY, UNIFORM(0, 3000, RANDOM()), '2014-01-01'); 

SELECT DATEDIFF(DAY, '2014-01-01', CURRENT_DATE);

SELECT DATEADD(DAY, UNIFORM(0, 3000, RANDOM()), '2014-01-01');


--- 2. Update the store table so that stores with StoreID between 91 and 100
--- are opened in the last 12 months.


SELECT * FROM DIMSTORE 
WHERE STOREID BETWEEN 91 AND 100;

SELECT DATEADD(YEAR, -1, CURRENT_DATE);

SELECT DATEADD(DAY, UNIFORM(0, 360, RANDOM()), '2024-11-16');

UPDATE DIMSTORE SET STOREOPENINGDATE = DATEADD(DAY, UNIFORM(0, 360, RANDOM()), '2024-11-16') WHERE STOREID BETWEEN 91 AND 100;

-- 3. Update the customer table so that all customers are atleast 12 year old.
-- Any customer that is less than 12 years old.
-- Substract 12 years from their DOB.

SELECT * FROM DIMCUSTOMER WHERE DATEOFBIRTH >= DATEADD(YEAR, -10, CURRENT_DATE);
SELECT DATEADD(YEAR, -12, CURRENT_DATE);

--4. We may have some orders in the FactOrders table that may have DateID
-- Which contains a value even before the store was opened.
-- For example: A store was opened last year but we have an order from 10
-- years ago which is incorrect.
-- Update DateID in order table for such rows to have a random DateID after 
-- the opening date of their respective stores.

-- solution--
-- First, We identify the orders that have a problem
-- Identify a valid date that we can enter
-- we need to convert the new_date to dateid
-- we perform the update.

update FACTORDERS F
set f.dateid = r.dateid from
(SELECT orderid, d.dateid from
(SELECT orderid,
DATEADD(DAY,
DATEDIFF(DAY,S.STOREOPENINGDATE,CURRENT_DATE) * UNIFORM(1, 10, RANDOM()) * .1,  S.STOREOPENINGDATE) AS NEW_DATE
FROM FACTORDERS F
JOIN DIMDATE D ON F.DATEID = D.DATEID
JOIN DIMSTORE S ON F.STOREID = S.STOREID
WHERE D.DATE < S.STOREOPENINGDATE) o
join dimdate d on o.New_Date = d.date) r
where f.orderid = r.orderid;

commit;
select * from factorders;

-- 5. List customers who haven't placed an order in the last 30 days.


select * from dimcustomer where customerid not in (
select distinct c.customerid 
from dimcustomer c
join factorders f on c.customerid = f.customerid
join dimdate d on f.dateid = d.dateid
where d.date >= dateadd(month, -1, current_date));


-- 6. List the store that was opened most recently along with the sales since then.

select * from dimstore;

with store_rank as(

select storeid, storeopeningdate, row_number() over (order  by storeopeningdate desc) as final_rank 
from dimstore
), 
most_recent_store as 
(
select storeid from store_rank where final_rank = 1
),

store_amount as 
(
select o.storeid, sum(totalamount) as totalamount from factorders o
join most_recent_store s
on o.storeid = s.storeid
group by o.storeid
)
select s.*, a.totalamount from dimstore s
join store_amount a on s.storeid = a.storeid;


-- 7. Find customers who have ordered product from more than 3 categories in the last 12 months.

with base_data as (
select o.customerid, p.category from factorders o
join dimdate d on o.dateid = o.dateid
join dimproduct p on o.productid = p.productid
where d.date >=dateadd(month, -12, current_date)
group by o.customerid, p.category
)
select customerid
from base_data
group by customerid
having count(distinct category)>3;

-- 8. Get the monthly total sales for the current year.

select * from dimdate;

select month, sum(totalamount) as Monthly_Total_Sales from factorders o
join dimdate d on o.dateid = d.dateid
where d.year = extract(year from date '2024-11-17')
group by month
order by month;


-- 9. Find the highest discount given on any order in the last 1 year.

with base_data as
(
select discountamount, row_number() over(order by discountamount desc) as discountamount_rank 
from factorders o
join dimdate d on o.dateid = d.dateid
where d.date >= dateadd(year, -1, date '2024-11-17')
)
select * from base_data where discountamount_rank = 1;

-- 10. Calculate total sales by multiplying the unit price from product column with quantity ordered from factorders.

select sum(quantityordered * unitprice) from factorders o 
join dimproduct p on o.productid = p.productid;

-- 11. Show the customer id of the customer who as taken the maximum discount in their lifetime.

select customerid from factorders o
group by customerid
order by sum(discountamount) desc limit 1;

-- 12. List the customer who placed the maximum number of orders till date.

with base_data as
(
select customerid, count(orderid) as order_count from factorders f
group by customerid
),
order_rank_data as
(
select b.*, row_number() over( order by order_count desc) as order_rank from base_data b
)
select customerid, order_count from order_rank_data where order_rank = 1;


-- 13. Show the top 3 brands based on their sales in the last year.

with brand_sales as
(
select brand, sum(totalamount) as total_sales
from factorders f 
join dimdate d on f.dateid = d.dateid
join dimproduct p on f.productid = p.productid
where d.date >= dateadd(year, -5, current_date)
group by brand
),
brand_sales_rank as
(
select b.*, row_number() over (order by total_sales desc) as sales_rank from brand_sales b
)
select brand, total_sales from brand_sales_rank where sales_rank <= 3;

-- 14. If the discount amount and the shipping cost was made static at 5 and 8% respectively.
----- will the sum of new total amount be greater than the total amount we have?

select case when sum(orderamount - orderamount*.05 - orderamount*.08) > sum(totalamount) 
then 'yes' else 'no' end from factorders f;

-- 15. Share the number of customers and their current loyalty program status.
select * from dimcustomer c;
select * from dimloyaltyprogram l;

select programtier, count(customerid) as customer_count from dimcustomer c
join dimloyaltyprogram l on c.loyaltyprogramid = l.loyaltyprogramid
group by programtier;

-- 16. Show the region category wise total amount for the last 12 months.

select region, category, sum(totalamount) as total_sales
from factorders f
join dimdate d on f.dateid = d.dateid
join dimproduct p on p.productid = f.productid
join dimstore s on f.storeid = s.storeid
where d.date >= dateadd(month, -12, current_date)
group by region, category
order by total_sales desc;

-- 17. Show the top 5 products based on quantity ordered in the last 3 years

select * from dimproduct;

with quantity_ordered as
(
select f.productid,  sum(quantityordered) as totalQuantity
from factorders f
join dimdate d on f.dateid = d.dateid
where d.date >= dateadd(year, -3, current_date)
group by f.productid
),
quantity_rank_data as 
(
select q.*, row_number() over (order by totalQuantity desc) as quantity_wise_rank from quantity_ordered q
)
select productid, totalQuantity from quantity_rank_data where quantity_wise_rank <=5;

-- 18. List total amount for each programtier since year 2023

select l.programname, sum(totalamount) as total_sales
from factorders f
join dimdate d on f.dateid = f.dateid
join dimcustomer c on f.customerid = c.customerid
join dimloyaltyprogram l on c.loyaltyprogramid = l.loyaltyprogramid
where d.year >= 2023
group by l.programname;


-- 19. Calculate the revenue generated by each store manager in June 2024.

select s.managername, sum(totalamount) as total_sales
from factorders f
join dimdate d on f.dateid = f.dateid
join dimstore s on f.storeid = s.storeid
where d.year = 2024 and d.month = 6
group by s.managername
order by total_sales desc;

-- 20. List the average order amount per store, along with the store name and type for the year 2024.

select s.storename, s.storetype, avg(totalamount) as total_sales
from factorders f
join dimdate d on f.dateid = f.dateid
join dimstore s on f.storeid = s.storeid
where d.year = 2024
group by s.storename, s.storetype;

-- 21. Reading from files
------ Query data from the customer csv file that is present in the stage.


--- verify the files exist in the stage

LIST @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/DimCustomerData/;

select $1, $2, $3
from @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/DimCustomerData/
(FILE_FORMAT => 'CSV_SOURCE_FILE_FORMAT');

--- 22. Aggregation in file
------ Aggregate data, share the count of records in the DimCustomer file from stage.

select count($1)
from @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/DimCustomerData/
(FILE_FORMAT => 'CSV_SOURCE_FILE_FORMAT');

--- 23. Filter from files
----- Filter data, share the records from DimCustomer file where costomerid is greater than 960.

select $1, $2, $3, $4, $5, $6
from @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/DimCustomerData/
(FILE_FORMAT => 'CSV_SOURCE_FILE_FORMAT')
where $4 > '2000-01-01';

--- 24. Join dimcustomer and dimloyaltyprogram and show the customer 1st name
------ along with the program tier they are part of.

with customer_data as
(
select $1 as FirstName, $12 as Loyalty_Program_ID
from @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/DimCustomerData/
(FILE_FORMAT => 'CSV_SOURCE_FILE_FORMAT')
),
loyalty_data as
(
select $1 as loyalty_program_ID, $3 as program_tier
from @TEST_DB.TEST_DB_SCHEMA.TESTSTAGE/DimLoyaltyInfo/
(FILE_FORMAT => 'CSV_SOURCE_FILE_FORMAT')
)
select program_tier, count(1) as total_count from customer_data c 
join loyalty_data l on c.Loyalty_Program_ID = l.Loyalty_Program_ID
group by program_tier;
