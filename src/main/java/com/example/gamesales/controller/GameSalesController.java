package com.example.gamesales.controller;


import com.example.gamesales.constants.GameSalesConstants;
import com.example.gamesales.entity.GameSalesParamsEntity;
import com.example.gamesales.entity.TotalSalesParamsEntity;
import com.example.gamesales.service.GameSalesService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.HashMap;

@RestController
@Slf4j
public class GameSalesController {

    private final GameSalesService gameSalesService;

    public GameSalesController(GameSalesService gameSalesService) {
        this.gameSalesService = gameSalesService;
    }

    @PostMapping("/import")
    public ResponseEntity importCsv(@RequestParam MultipartFile csvFile) {
        int totalRecordCount = gameSalesService.validateCsvFile(csvFile);
        gameSalesService.save(csvFile, totalRecordCount);
        return ResponseEntity.ok().body("csv import success");
    }

    @GetMapping("/getGameSales")
    public ResponseEntity getGameSales(
            @RequestParam(name = "params", required = false) String params,
            @PageableDefault(size = 100) Pageable pageable,
            @RequestParam(required = false, defaultValue = GameSalesConstants.DATE_OF_SALE_COLUMN_NAME) String sortField,
            @RequestParam(required = false, defaultValue = GameSalesConstants.SORT_DIR_DESC) String sortDir) {
        GameSalesParamsEntity gameSalesParamsEntity = gameSalesService.validateGetGameSalesRequest(params, sortField, sortDir);
//         return ResponseEntity.ok().body(gameSalesService.getGameSalesPageWith(gameSalesParamsEntity, sortField, sortDir, pageable));
        return ResponseEntity.ok().body(gameSalesService.getGameSalesWith(gameSalesParamsEntity, sortField, sortDir, pageable.getPageNumber(), pageable.getPageSize()));
    }

    @GetMapping("/getTotalSales")
    public ResponseEntity getTotalSales(@RequestParam String params) {
        TotalSalesParamsEntity totalSalesParamsEntity = gameSalesService.validateTotalSalesRequest(params);
        HashMap<String, Object> response = gameSalesService.getTotalSalesWith(totalSalesParamsEntity);
        return ResponseEntity.ok().body(response);
    }
}
