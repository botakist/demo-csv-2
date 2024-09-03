-- create db functions

DROP FUNCTION if exists calc_total_sales_between;
DROP FUNCTION if exists calc_total_sales_between_for_game_no;
DROP FUNCTION if exists count_game_sales_between;
DELIMITER $$
CREATE
DEFINER = `root`@`localhost` FUNCTION `calc_total_sales_between`(from_date DATE, to_date DATE) RETURNS double
    DETERMINISTIC
BEGIN
    DECLARE total_sales double;
SELECT SUM(sale_price)
INTO total_sales
FROM game_sales
WHERE date_of_sale BETWEEN from_date AND to_date;

RETURN total_sales;
END $$

CREATE
DEFINER = `root`@`localhost` FUNCTION `calc_total_sales_between_for_game_no`(from_date DATE, to_date DATE, gameno INT) RETURNS double
    DETERMINISTIC
BEGIN
    DECLARE total_sales double;
SELECT SUM(sale_price)
INTO total_sales
FROM game_sales
WHERE date_of_sale BETWEEN from_date AND to_date
  AND game_no = gameno;
RETURN total_sales;
END $$

CREATE
DEFINER = `root`@`localhost` FUNCTION `count_game_sales_between`(from_date DATE, to_date DATE) RETURNS bigint
    DETERMINISTIC
BEGIN
    DECLARE games_sales_count BIGINT;
SELECT COUNT(*)
INTO games_sales_count
FROM game_sales
WHERE date_of_sale BETWEEN from_date AND to_date;

RETURN games_sales_count;
END $$

DELIMITER ;