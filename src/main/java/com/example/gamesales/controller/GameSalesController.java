package com.example.gamesales.controller;


import com.example.gamesales.constants.GameSalesConstants;
import com.example.gamesales.entity.GameSalesParamsEntity;
import com.example.gamesales.entity.TotalSalesParamsEntity;
import com.example.gamesales.service.GameSalesService;
import com.example.gamesales.validators.ValidatorService;
import com.example.gamesales.view.GameSalesView;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.HashMap;
import java.util.List;

@RestController
@Slf4j
public class GameSalesController {

    private final GameSalesService gameSalesService;
    private final ValidatorService validatorService;

    @Autowired
    public GameSalesController(GameSalesService gameSalesService, ValidatorService validatorService) {
        this.gameSalesService = gameSalesService;
        this.validatorService = validatorService;
    }

    @PostMapping("/import")
    public ResponseEntity<String> importCsv(@RequestParam MultipartFile csvFile) {
        int totalRecordCount = validatorService.validateCsvFile(csvFile);
        gameSalesService.save(csvFile, totalRecordCount);
        return ResponseEntity.ok().body("csv import success.");
    }

    @GetMapping("/getGameSales")
    public ResponseEntity<List<GameSalesView>> getGameSales(
            @RequestParam(name = "params", required = false) String params,
            @PageableDefault(size = 100) Pageable pageable,
            @RequestParam(required = false, defaultValue = GameSalesConstants.DATE_OF_SALE_COLUMN_NAME) String sortField,
            @RequestParam(required = false, defaultValue = GameSalesConstants.SORT_DIR_DESC) String sortDir) {
        GameSalesParamsEntity gameSalesParamsEntity = validatorService.validateGetGameSalesRequest(params, sortField, sortDir);
        return ResponseEntity.ok().body(gameSalesService.getGameSalesWith(gameSalesParamsEntity, sortField, sortDir, pageable.getPageNumber(), pageable.getPageSize()));
    }

    @GetMapping("/getTotalSales")
    public ResponseEntity<HashMap<String, Object>> getTotalSales(@RequestParam String params) {
        TotalSalesParamsEntity totalSalesParamsEntity = validatorService.validateTotalSalesRequest(params);
        HashMap<String, Object> response = gameSalesService.getTotalSalesWith(totalSalesParamsEntity);
        return ResponseEntity.ok().body(response);
    }
}
