package com.example.gamesales.task;

import com.example.gamesales.service.BatchInsertService;
import com.example.gamesales.service.ProgressTrackingService;
import com.example.gamesales.view.InvalidRecordView;
import com.example.gamesales.view.ProgressTrackingView;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class BatchInsertInvalidRecordsTask  implements Callable<Void>  {

    private final List<InvalidRecordView> batch;
    private final BatchInsertService batchInsertService;
    private final AtomicInteger progressTracking;
    private final ProgressTrackingService progressTrackingService;
    private final ProgressTrackingView progressTrackingView;

    public BatchInsertInvalidRecordsTask(BatchInsertService batchInsertService, List<InvalidRecordView> batch, AtomicInteger progressTracking, ProgressTrackingService progressTrackingService, ProgressTrackingView progressTrackingView) {
        this.batch = batch;
        this.batchInsertService = batchInsertService;
        this.progressTracking = progressTracking;
        this.progressTrackingService = progressTrackingService;
        this.progressTrackingView = progressTrackingView;
    }

    @Override
    public Void call() {
        try {
            // Perform the batch insert
            batchInsertService.batchInsertInvalidRecords(batch, progressTrackingView.getId());
            // Update the progress tracking count after the batch insert completes
            int invalidRecordsBatchSize = progressTracking.addAndGet(batch.size());
            progressTrackingView.setInvalidRecordsCount(invalidRecordsBatchSize);
            progressTrackingService.updateProgress(progressTrackingView);
        } catch (Exception e) {
            log.error("Batch insert failed", e);
            progressTrackingView.setStatus("FAILED");
            progressTrackingService.updateProgress(progressTrackingView);
        }
        return null;
    }
}
