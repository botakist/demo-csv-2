DELIMITER $$
CREATE DEFINER=`root`@`localhost` FUNCTION `count_game_sales_between`(from_date DATE, to_date DATE) RETURNS bigint
    DETERMINISTIC
BEGIN
	DECLARE games_sales_count BIGINT;
SELECT
    COUNT(*)
INTO games_sales_count FROM
    game_sales
WHERE
    date_of_sale BETWEEN from_date AND to_date;

RETURN games_sales_count;
END$$
DELIMITER;
