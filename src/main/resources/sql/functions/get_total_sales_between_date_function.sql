CREATE DEFINER=`root`@`localhost` FUNCTION `calc_total_sales_between`(from_date DATE, to_date DATE) RETURNS double
    DETERMINISTIC
BEGIN
	DECLARE total_sales double;
SELECT
    SUM(sale_price)
INTO total_sales FROM
    game_sales
WHERE
    date_of_sale BETWEEN from_date AND to_date;

RETURN total_sales;
END