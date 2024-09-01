package com.example.gamesales.task;

import com.example.gamesales.service.BatchInsertService;
import com.example.gamesales.view.InvalidRecordView;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.Callable;

@Slf4j
public class BatchInsertInvalidRecordsTask  implements Callable<Void>  {

    private final List<InvalidRecordView> batch;
    private final BatchInsertService batchInsertService;

    public BatchInsertInvalidRecordsTask(BatchInsertService batchInsertService, List<InvalidRecordView> batch) {
        this.batch = batch;
        this.batchInsertService = batchInsertService;
    }

    @Override
    public Void call() {
        batchInsertService.batchInsertInvalidRecords(batch);
        return null;
    }
}
