package com.example.gamesales.task;

import com.example.gamesales.exception.ValidationException;
import com.example.gamesales.view.GameSalesView;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class BatchInsertGameSalesTask implements Callable<Void> {

    private final List<GameSalesView> batch;
    private final JdbcTemplate jdbcTemplate;
    private final AtomicInteger progressTracker;

    public BatchInsertGameSalesTask(List<GameSalesView> batch, JdbcTemplate jdbcTemplate, AtomicInteger progressTracker) {
        this.batch = batch;
        this.jdbcTemplate = jdbcTemplate;
        this.progressTracker = progressTracker;
    }

    @Override
    public Void call() {
        batchInsertGameSales(batch);
        progressTracker.addAndGet(batch.size());
        return null;
    }

    private void batchInsertGameSales(List<GameSalesView> views) {
        String sql = "INSERT INTO game_sales (id, game_no, game_name, game_code, type, cost_price, tax, sale_price, date_of_sale) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        List<Object[]> batchArgs = views.stream()
                .map(view -> new Object[]{
                        view.getId(),
                        view.getGameNo(),
                        view.getGameName(),
                        view.getGameCode(),
                        view.getType(),
                        view.getCostPrice(),
                        view.getTax(),
                        view.getSalePrice(),
                        view.getDateOfSale()
                })
                .collect(Collectors.toList());
        try {
            jdbcTemplate.batchUpdate(sql, batchArgs);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new ValidationException(e.getMessage());
        }
    }
}
