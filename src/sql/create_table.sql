drop table if exists game_sales;
CREATE TABLE `game_sales` (
                              `id` bigint NOT NULL,
                              `game_no` int NOT NULL,
                              `game_name` varchar(20) NOT NULL,
                              `game_code` varchar(5) NOT NULL,
                              `type` int NOT NULL,
                              `cost_price` double NOT NULL,
                              `tax` double NOT NULL,
                              `sale_price` double NOT NULL,
                              `date_of_sale` timestamp(6) NOT NULL,
                              PRIMARY KEY (`id`),
                              KEY `idx_game_sales_game_no` (`game_no`),
                              KEY `idx_game_sales_sale_price_date_of_sale` (`date_of_sale`,`sale_price`),
                              KEY `idx_game_sales_sale_price_date_of_sale_game_no` (`date_of_sale`,`game_no`,`sale_price`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci