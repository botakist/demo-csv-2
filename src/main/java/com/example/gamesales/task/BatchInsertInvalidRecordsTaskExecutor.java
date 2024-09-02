package com.example.gamesales.task;

import com.example.gamesales.service.BatchInsertService;
import com.example.gamesales.service.ProgressTrackingService;
import com.example.gamesales.view.InvalidRecordView;
import com.example.gamesales.view.ProgressTrackingView;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchInsertInvalidRecordsTaskExecutor {
    private final ExecutorService executorService;
    private final BatchInsertService batchInsertService;
    private final ProgressTrackingService progressTrackingService;

    public BatchInsertInvalidRecordsTaskExecutor(ExecutorService executorService, BatchInsertService batchInsertService, ProgressTrackingService progressTrackingService) {
        this.executorService = executorService;
        this.batchInsertService = batchInsertService;
        this.progressTrackingService = progressTrackingService;
    }

    public List<Future<Void>> executeBatchInvalidRecordInserts(List<InvalidRecordView> invalidRecordViewList, int batchSize, AtomicInteger progressTracker, ProgressTrackingView view) {
        int totalCount = invalidRecordViewList.size();
        List<Future<Void>> futures = new ArrayList<>();

        for (int i = 0; i < totalCount; i += batchSize) {
            int endIndex = Math.min(i + batchSize, totalCount);
            // get sublist for current batch
            List<InvalidRecordView> batchList = invalidRecordViewList.subList(i, endIndex);
            Future<Void> future = executorService.submit(new BatchInsertInvalidRecordsTask(batchInsertService, batchList, progressTracker, progressTrackingService, view));
            futures.add(future);
        }
        return futures;
    }
}
