package com.example.gamesales.task;

import com.example.gamesales.service.BatchInsertService;
import com.example.gamesales.view.GameSalesView;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.Callable;

@Slf4j
public class BatchInsertGameSalesTask implements Callable<Void> {
    private final BatchInsertService batchInsertService;
    private final List<GameSalesView> batch;


    public BatchInsertGameSalesTask(BatchInsertService batchInsertService, List<GameSalesView> batch) {
        this.batchInsertService = batchInsertService;
        this.batch = batch;
    }

    @Override
    public Void call() {
        batchInsertService.batchInsertGameSales(batch);
        return null;
    }
}
