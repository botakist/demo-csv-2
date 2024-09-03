CREATE DEFINER=`root`@`localhost` FUNCTION `calc_total_sales_between_for_game_no`(from_date DATE, to_date DATE, gameno INT) RETURNS double
    DETERMINISTIC
BEGIN
	DECLARE total_sales double;
SELECT
    SUM(sale_price)
INTO total_sales FROM
    game_sales
WHERE
    date_of_sale BETWEEN from_date AND to_date
  AND game_no = gameno;
RETURN total_sales;
END