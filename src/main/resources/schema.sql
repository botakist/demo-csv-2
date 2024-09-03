drop table if exists game_sales, progress_tracking, invalid_record;
-- create game_sales table
CREATE TABLE `game_sales`
(
    `id`           bigint       NOT NULL AUTO_INCREMENT,
    `cost_price`   double       NOT NULL,
    `date_of_sale` datetime(6)  NOT NULL,
    `game_code`    varchar(255) NOT NULL,
    `game_name`    varchar(255) NOT NULL,
    `game_no`      int          NOT NULL,
    `sale_price`   double       NOT NULL,
    `tax`          double       NOT NULL,
    `type`         int          NOT NULL,
    PRIMARY KEY (`id`),
    KEY `idx_date_of_sale` (`date_of_sale`),
    KEY `idx_date_of_sale_sale_price` (`date_of_sale`, `sale_price`),
    KEY `idx_date_of_sale_sale_price_game_no` (`date_of_sale`, `sale_price`, `game_no`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;

-- create progress_tracking table
CREATE TABLE `progress_tracking`
(
    `id`                            bigint NOT NULL AUTO_INCREMENT,
    `end_time`                      datetime(6)  DEFAULT NULL,
    `invalid_records_count`         int          DEFAULT NULL,
    `start_time`                    datetime(6)  DEFAULT NULL,
    `status`                        varchar(255) DEFAULT NULL,
    `total_processed_records_count` int          DEFAULT NULL,
    `total_records_count`           int          DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;

-- create invalid record table
CREATE TABLE `invalid_record`
(
    `id`                      bigint       NOT NULL AUTO_INCREMENT,
    `created_on`              datetime(6)  NOT NULL,
    `progress_track_view_id`  bigint       NOT NULL,
    `invalid_record_row_id`   bigint       NOT NULL,
    `invalid_record_row_text` varchar(255) NOT NULL,
    PRIMARY KEY (`id`),
    KEY `fk_progress_track_view_id` (`progress_track_view_id`),
    CONSTRAINT `fk_progress_track_view_id` FOREIGN KEY (`progress_track_view_id`) REFERENCES `progress_tracking` (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;

-- create sequences
drop table if exists prog_trk_seq;
CREATE TABLE `prog_trk_seq`
(
    `next_val` bigint DEFAULT 1
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;

insert into prog_trk_seq (next_val)
values (1);

drop table if exists game_sales_seq;
CREATE TABLE `game_sales_seq`
(
    `next_val` bigint DEFAULT 1
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;
insert into game_sales_seq (next_val)
values (1);

drop table if exists invalid_record_seq;
CREATE TABLE `invalid_record_seq`
(
    `next_val` bigint DEFAULT 1
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;
insert into invalid_record_seq (next_val)
values (1);
