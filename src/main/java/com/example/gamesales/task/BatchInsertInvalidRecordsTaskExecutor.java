package com.example.gamesales.task;

import com.example.gamesales.view.InvalidRecordView;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchInsertInvalidRecordsTaskExecutor {
    private final ExecutorService executorService;
    private final JdbcTemplate jdbcTemplate;
    public BatchInsertInvalidRecordsTaskExecutor(ExecutorService executorService, JdbcTemplate jdbcTemplate) {
        this.executorService = executorService;
        this.jdbcTemplate = jdbcTemplate;
    }

    public void executeBatchInvalidRecordInserts(List<InvalidRecordView> invalidRecordViewList, int batchSize, AtomicInteger progressTracker) {
        int totalCount = invalidRecordViewList.size();
        for (int i = 0; i < totalCount; i += batchSize) {
            int endIndex = Math.min(i + batchSize, totalCount);
            // get sublist for current batch
            List<InvalidRecordView> batchList = invalidRecordViewList.subList(i, endIndex);
            executorService.submit(new BatchInsertInvalidRecordsTask(batchList, jdbcTemplate, progressTracker));
        }
    }
}
