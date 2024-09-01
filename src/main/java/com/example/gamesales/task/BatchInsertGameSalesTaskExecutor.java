package com.example.gamesales.task;

import com.example.gamesales.service.BatchInsertService;
import com.example.gamesales.service.ProgressTrackingService;
import com.example.gamesales.view.GameSalesView;
import com.example.gamesales.view.ProgressTrackingView;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchInsertGameSalesTaskExecutor {

    private final ExecutorService executorService;
    private final BatchInsertService batchInsertService;
    private final ProgressTrackingService progressTrackingService;

    public BatchInsertGameSalesTaskExecutor(ExecutorService executorService, BatchInsertService batchInsertService, ProgressTrackingService progressTrackingService) {
        this.executorService = executorService;
        this.batchInsertService = batchInsertService;
        this.progressTrackingService = progressTrackingService;
    }


    public void executeBatchGameSalesInserts(List<GameSalesView> gameSalesViewList, int batchSize, AtomicInteger progressTracking, ProgressTrackingView view) {
        int totalCount = gameSalesViewList.size();
        for (int i = 0; i < totalCount; i += batchSize) {
            int endIndex = Math.min(i + batchSize, totalCount);
            // get sublist for current batch
            List<GameSalesView> batchList = gameSalesViewList.subList(i, endIndex);
            executorService.submit(new BatchInsertGameSalesTask(batchInsertService, batchList));
            view.setTotalProcessedRecordsCount(progressTracking.addAndGet(batchList.size()));
            progressTrackingService.updateProgress(view);
        }
        view.setStatus("COMPLETED");
        view.setEndTime(LocalDateTime.now());
        progressTrackingService.updateProgress(view);
    }

}
