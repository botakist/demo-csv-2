select count_game_sales_between('2024-04-01T00:00:00','2024-04-30T00:00:00');
select calc_total_sales_between('2024-04-01T00:00:00','2024-04-30T00:00:00');
select calc_total_sales_between_for_game_no('2024-04-01T00:00:00','2024-04-02T00:00:00', 1);
select sum(sale_price) from game_sales where date_of_sale between '2024-04-01T00:00:00' and '2024-04-02T00:00:00';
select sum(sale_price) from game_sales where date_of_sale between '2024-04-01T00:00:00' and '2024-04-02T00:00:00' and game_no = 1;

select max(date_of_sale) from game_sales;
select max(date_of_sale) from game_sales where date_of_sale between '2024-04-01T00:00:00Z' and '2024-04-30T00:00:00Z';
select * from game_sales order by date_of_sale desc;
SELECT * FROM game_sales WHERE 1=1 ORDER BY id DESC LIMIT 100 OFFSET 0;
select * from game_sales where 1=1  AND date_of_sale >= '2024-04-01T00:00:00Z'  AND date_of_sale <= '2024-04-02T00:00:00Z' AND sale_price <= 10 order by date_of_sale desc limit 100 offset 100;
select * from game_sales order by date_of_sale desc limit 100 offset 0;