package com.example.gamesales.task;

import com.example.gamesales.exception.ValidationException;
import com.example.gamesales.view.InvalidRecordView;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class BatchInsertInvalidRecordsTask  implements Callable<Void>  {

    private final List<InvalidRecordView> batch;
    private final JdbcTemplate jdbcTemplate;
    private final AtomicInteger progressTracker;

    public BatchInsertInvalidRecordsTask(List<InvalidRecordView> batch, JdbcTemplate jdbcTemplate, AtomicInteger progressTracker) {
        this.batch = batch;
        this.jdbcTemplate = jdbcTemplate;
        this.progressTracker = progressTracker;
    }

    @Override
    public Void call() {
        batchInsertInvalidRecords(batch);
        progressTracker.addAndGet(batch.size());
        return null;
    }


    public void batchInsertInvalidRecords(List<InvalidRecordView> views) {
        String sql = "INSERT INTO invalid_record (invalid_record_row_id, invalid_record_row_text, created_on) VALUES (?, ?, ?)";
        List<Object[]> batchArgs = views.stream()
                .map(view -> new Object[]{
                        view.getInvalidRecordRowId(),
                        view.getInvalidRecordRowText(),
                        view.getCreatedOn()
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
