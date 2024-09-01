package com.example.gamesales.task;

import com.example.gamesales.view.GameSalesView;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchInsertGameSalesTaskExecutor {

    private final JdbcTemplate jdbcTemplate;

    private final ExecutorService executorService;

    public BatchInsertGameSalesTaskExecutor(ExecutorService executorService, JdbcTemplate jdbcTemplate) {
        this.executorService = executorService;
        this.jdbcTemplate = jdbcTemplate;
    }


    public void executeBatchGameSalesInserts(List<GameSalesView> gameSalesViewList, int batchSize, AtomicInteger progressTracker) {
        int totalCount = gameSalesViewList.size();
        for (int i = 0; i < totalCount; i += batchSize) {
            int endIndex = Math.min(i + batchSize, totalCount);
            // get sublist for current batch
            List<GameSalesView> batchList = gameSalesViewList.subList(i, endIndex);
            executorService.submit(new BatchInsertGameSalesTask(batchList, jdbcTemplate, progressTracker));
        }
    }

}
