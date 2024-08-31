DROP PROCEDURE IF EXISTS GenerateAndInsertMillionRows;

DELIMITER $$

CREATE PROCEDURE GenerateAndInsertMillionRows()
BEGIN
    DECLARE i INT DEFAULT 1;

    WHILE i <= 1000000
        DO
            INSERT INTO `game_sales`
            (`id`,
             `game_no`,
             `game_name`,
             `game_code`,
             `type`,
             `cost_price`,
             `tax`,
             `sale_price`,
             `date_of_sale`)
            VALUES (i,
                    FLOOR(1 + (RAND() * 100)),
                    CAST(concat(
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97)
                         ) AS CHAR),
                    CAST(concat(
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97),
                            char(round(rand() * 25) + 97)
                         ) AS CHAR),
                    FLOOR(1 + RAND() * 2),
                    @cost_price := FLOOR(1 + RAND() * 100),    -- Random cost_price between 1 and 100
                    0.09,                                      -- Constant tax at 9%
                    @cost_price + @cost_price * 0.09,
                        TIMESTAMPADD(SECOND, FLOOR(RAND() * TIMESTAMPDIFF(SECOND, '2024-04-01 00:00:00', '2024-04-30 23:59:59')), '2024-04-01 00:00:00'));
            SET i = i + 1;
        END WHILE;
END$$

DELIMITER ;
