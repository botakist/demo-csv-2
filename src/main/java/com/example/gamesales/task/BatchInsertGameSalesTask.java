package com.example.gamesales.task;

import com.example.gamesales.service.BatchInsertService;
import com.example.gamesales.service.ProgressTrackingService;
import com.example.gamesales.view.GameSalesView;
import com.example.gamesales.view.ProgressTrackingView;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class BatchInsertGameSalesTask implements Callable<Void> {
    private final BatchInsertService batchInsertService;
    private final List<GameSalesView> batch;
    private final AtomicInteger progressTracking;
    private final ProgressTrackingService progressTrackingService;
    private final ProgressTrackingView progressTrackingView;

    public BatchInsertGameSalesTask(BatchInsertService batchInsertService, List<GameSalesView> batch, AtomicInteger progressTracking, ProgressTrackingService progressTrackingService, ProgressTrackingView progressTrackingView) {
        this.batchInsertService = batchInsertService;
        this.batch = batch;
        this.progressTracking = progressTracking;
        this.progressTrackingService = progressTrackingService;
        this.progressTrackingView = progressTrackingView;
    }

    @Override
    public Void call() {
        try {
            // batch insert
            batchInsertService.batchInsertGameSales(batch);
            // Update the progress tracking count after the batch insert completes
            int processedRecords = progressTracking.addAndGet(batch.size());
            progressTrackingView.setTotalProcessedRecordsCount(processedRecords);
            progressTrackingService.updateProgress(progressTrackingView);
        } catch (Exception e) {
            log.error("Batch insert failed", e);
            progressTrackingView.setStatus("FAILED");
            progressTrackingService.updateProgress(progressTrackingView);
        }
        return null;
    }
}
