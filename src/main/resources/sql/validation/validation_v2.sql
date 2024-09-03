-- for validating importCsv endpoint
select * from progress_tracking;

select count(*) from game_sales;
select count(*) from invalid_record;

-- invalid_record table
select * from invalid_record;

-- for validating /getGameSales without any parameters passed
-- page 1, expect 1st result id to be same as postman response
select * from game_sales order by date_of_sale desc limit 100 offset 0;
-- page 2, expect 1st result id to be same as postman response
select * from game_sales order by date_of_sale desc limit 100 offset 1;

-- custom
select * from game_sales order by date_of_sale asc limit 100 offset X;
