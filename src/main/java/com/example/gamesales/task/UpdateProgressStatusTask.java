package com.example.gamesales.task;

import com.example.gamesales.service.ProgressTrackingService;
import com.example.gamesales.view.ProgressTrackingView;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class UpdateProgressStatusTask implements Callable<Void> {

    private final List<Future<Void>> futures;
    private final ProgressTrackingView progressTrackingView;
    private final ProgressTrackingService progressTrackingService;

    public UpdateProgressStatusTask(List<Future<Void>> futures, ProgressTrackingView progressTrackingView, ProgressTrackingService progressTrackingService) {
        this.futures = futures;
        this.progressTrackingView = progressTrackingView;
        this.progressTrackingService = progressTrackingService;
    }

    @Override
    public Void call() throws Exception {
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                progressTrackingView.setStatus("ERROR");
                progressTrackingView.setEndTime(LocalDateTime.now());
                progressTrackingService.updateProgress(progressTrackingView);
                Thread.currentThread().interrupt();
                return null;
            }
        }
        // If all tasks are successful, update the progress to "COMPLETED"
        progressTrackingView.setStatus("COMPLETED");
        progressTrackingView.setEndTime(LocalDateTime.now());
        progressTrackingService.updateProgress(progressTrackingView);
        return null;
    }
}
