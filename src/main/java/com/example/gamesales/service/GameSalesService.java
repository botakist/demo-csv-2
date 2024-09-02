package com.example.gamesales.service;

import com.example.gamesales.constants.GameSalesConstants;
import com.example.gamesales.entity.GameSalesParamsEntity;
import com.example.gamesales.entity.TotalSalesParamsEntity;
import com.example.gamesales.exception.ValidationException;
import com.example.gamesales.repository.GameSalesRepository;
import com.example.gamesales.task.BatchInsertGameSalesTaskExecutor;
import com.example.gamesales.task.BatchInsertInvalidRecordsTaskExecutor;
import com.example.gamesales.task.UpdateProgressStatusTask;
import com.example.gamesales.util.GameSalesUtil;
import com.example.gamesales.validators.ValidatorService;
import com.example.gamesales.view.GameSalesView;
import com.example.gamesales.view.InvalidRecordView;
import com.example.gamesales.view.ProgressTrackingView;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class GameSalesService {
    private final GameSalesRepository gameSalesRepository;
    private final ExecutorService executorService;
    private final JdbcTemplate jdbcTemplate;
    private final BatchInsertService batchInsertService;
    private final ProgressTrackingService progressTrackingService;
    private final ValidatorService validatorService;

    @Value("${com.example.gamesales.import.threadpoolsize:20}")
    private int threadPoolSize;

    @Value("${spring.jpa.properties.hibernate.jdbc.batch_size:10000}")
    private int batchSize;

    @Autowired
    public GameSalesService(GameSalesRepository gameSalesRepository, ExecutorService executorService, JdbcTemplate jdbcTemplate, BatchInsertService batchInsertService, ProgressTrackingService progressTrackingService, ValidatorService validatorService) {
        this.gameSalesRepository = gameSalesRepository;
        this.executorService = executorService;
        this.jdbcTemplate = jdbcTemplate;
        this.batchInsertService = batchInsertService;
        this.progressTrackingService = progressTrackingService;
        this.validatorService = validatorService;
    }

    public void save(MultipartFile csvFile, int totalRecordsCount) {

        AtomicInteger validRecordsCount = new AtomicInteger();
        AtomicInteger invalidRecordsCount = new AtomicInteger();

        ProgressTrackingView progressTrackingView = progressTrackingService.initialiseProgressView(totalRecordsCount);

        List<GameSalesView> validGameSalesViews = new ArrayList<>();
        List<CSVRecord> invalidGameSalesRecords = new ArrayList<>();
        try (BufferedReader fileReader = new BufferedReader(new InputStreamReader(csvFile.getInputStream(), StandardCharsets.UTF_8));
             CSVParser csvParser = new CSVParser(fileReader, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim())) {
            Iterable<CSVRecord> csvRecords = csvParser.getRecords();

            for (CSVRecord csvRecord : csvRecords) {
                try {
                    // separate valid and invalid records first.
                    GameSalesView view = parseCsvLineToGameSalesView(csvRecord);
                    if (validatorService.isValidData(view)) {
                        validGameSalesViews.add(view);
                    } else {
                        invalidGameSalesRecords.add(csvRecord);
                    }
                } catch (NullPointerException | NumberFormatException | DateTimeParseException e) {
                    log.error(e.getMessage(), e);
                    throw new ValidationException("Error in parsing data in csv file.");
                }
            }

            BatchInsertGameSalesTaskExecutor executor = new BatchInsertGameSalesTaskExecutor(executorService, batchInsertService, progressTrackingService);
            List<Future<Void>> futures = executor.executeBatchGameSalesInserts(validGameSalesViews, batchSize, validRecordsCount, progressTrackingView);

            if (!invalidGameSalesRecords.isEmpty()) {
                BatchInsertInvalidRecordsTaskExecutor executor2 = new BatchInsertInvalidRecordsTaskExecutor(executorService, batchInsertService, progressTrackingService);
                futures.addAll(executor2.executeBatchInvalidRecordInserts(mapToInvalidRecordViews(invalidGameSalesRecords), batchSize, invalidRecordsCount, progressTrackingView));
            }

            executorService.submit(new UpdateProgressStatusTask(futures, progressTrackingView, progressTrackingService));

        } catch (IOException e) {
            log.error(e.getMessage(), e);
            progressTrackingView.setStatus("ERROR");
            progressTrackingView.setEndTime(LocalDateTime.now());
            progressTrackingService.updateProgress(progressTrackingView);
            throw new ValidationException("error reading data from csv file.");
        } finally {
            executorService.shutdown();
            // here
            try {
                if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                executorService.shutdownNow();
            }
        }
    }

    private List<InvalidRecordView> mapToInvalidRecordViews(List<CSVRecord> csvRecords) {
        List<InvalidRecordView> views = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();
        for (CSVRecord csvRecord : csvRecords) {
            InvalidRecordView view = new InvalidRecordView();
            view.setInvalidRecordRowId(csvRecord.getRecordNumber());
            view.setInvalidRecordRowText(csvRecord.toString());
            view.setCreatedOn(now);
            views.add(view);
        }
        return views;
    }

    private GameSalesView parseCsvLineToGameSalesView(CSVRecord csvRecord) throws NullPointerException, NumberFormatException, DateTimeParseException {
        GameSalesView view = new GameSalesView();
        view.setId(Long.parseLong(csvRecord.get(0)));
        view.setGameNo(Integer.parseInt(csvRecord.get(1)));
        view.setGameName(csvRecord.get(2));
        view.setGameCode(csvRecord.get(3));
        view.setType(Integer.parseInt(csvRecord.get(4)));
        view.setCostPrice(Double.parseDouble(csvRecord.get(5)));
        view.setTax(Double.parseDouble(csvRecord.get(6)));
        view.setSalePrice(Double.parseDouble(csvRecord.get(7)));
        view.setDateOfSale(LocalDateTime.parse(csvRecord.get(8), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")));
        return view;
    }

    public Page<GameSalesView> getGameSalesPageWith(GameSalesParamsEntity gameSalesParamsEntity, String sortField, String sortDir, Pageable pageable) {
        if (StringUtils.isNotBlank(sortField)) {
            if (GameSalesConstants.SORT_DIR_DESC.equals(sortDir)) {
                pageable = PageRequest.of(pageable.getPageNumber(), pageable.getPageSize(), Sort.by(sortField).descending());
            } else {
                pageable = PageRequest.of(pageable.getPageNumber(), pageable.getPageSize(), Sort.by(sortField).ascending());
            }
        }
        return gameSalesRepository.getGameSalesWithPage(gameSalesParamsEntity, pageable);
    }

    public List<GameSalesView> getGameSalesWith(GameSalesParamsEntity gameSalesParamsEntity, String sortField, String sortDirection, int page, int size) {
        if (page < 1) {
            page = 1;
        }
        int offset = (page - 1) * size;
        // base query
        StringBuilder sql = new StringBuilder("SELECT * FROM game_sales WHERE 1=1");
        // List to hold the sql query parameters
        List<Object> params = new ArrayList<>();
        if (gameSalesParamsEntity != null) {
            if (gameSalesParamsEntity.getFrom() != null) {
                sql.append(" AND date_of_sale >= ?");
                params.add(gameSalesParamsEntity.getFrom());
            }

            if (gameSalesParamsEntity.getTo() != null) {
                sql.append(" AND date_of_sale <= ?");
                params.add(gameSalesParamsEntity.getTo());
            }

            if (gameSalesParamsEntity.getMinPrice() != null) {
                sql.append(" AND sale_price >= ?");
                params.add(gameSalesParamsEntity.getMinPrice());
            }

            if (gameSalesParamsEntity.getMaxPrice() != null) {
                sql.append(" AND sale_price <= ?");
                params.add(gameSalesParamsEntity.getMaxPrice());
            }
        }

        if (StringUtils.isNotBlank(sortField)) {
            sql.append(" ORDER BY ").append(sortField);
        } else {
            sql.append(" ORDER BY ").append(GameSalesConstants.DATE_OF_SALE_COLUMN_NAME);
        }

        if (StringUtils.equalsIgnoreCase(GameSalesConstants.SORT_DIR_DESC, sortDirection)) {
            sql.append(" DESC");
        } else {
            sql.append(" ASC");
        }

        sql.append(" LIMIT ? OFFSET ?");
        params.add(size);
        params.add(offset);
        return jdbcTemplate.query(sql.toString(), params.toArray(), (rs, rowNum) -> {
            GameSalesView gameSalesView = new GameSalesView();
            gameSalesView.setId(rs.getLong(GameSalesConstants.ID));
            gameSalesView.setGameNo(rs.getInt("game_no"));
            gameSalesView.setGameName(rs.getString("game_name"));
            gameSalesView.setGameCode(rs.getString("game_code"));
            gameSalesView.setType(rs.getInt(GameSalesConstants.TYPE));
            gameSalesView.setCostPrice(rs.getDouble("cost_price"));
            gameSalesView.setTax(rs.getDouble(GameSalesConstants.TAX));
            gameSalesView.setSalePrice(rs.getDouble("sale_price"));
            gameSalesView.setDateOfSale(GameSalesUtil.convertTimestampToLocalDateTime(rs.getTimestamp("date_of_sale")));
            return gameSalesView;
        });
    }


    public HashMap<String, Object> getTotalSalesWith(TotalSalesParamsEntity totalSalesParamsEntity) {
        HashMap<String, Object> response = new HashMap<>();
        if (totalSalesParamsEntity.getFrom() != null) {
            response.put(GameSalesConstants.FROM, totalSalesParamsEntity.getFrom());
        }
        if (totalSalesParamsEntity.getTo() != null) {
            response.put(GameSalesConstants.TO, totalSalesParamsEntity.getTo());
        }
        if (StringUtils.isNotBlank(totalSalesParamsEntity.getGameNo())) {
            response.put(GameSalesConstants.GAME_NO, totalSalesParamsEntity.getGameNo());
        }
        if (StringUtils.isNotBlank(totalSalesParamsEntity.getCategory())) {
            response.put(GameSalesConstants.CATEGORY, totalSalesParamsEntity.getCategory());
            if (GameSalesConstants.CATEGORY_TOTAL_SALES.equals(totalSalesParamsEntity.getCategory())) {
                if (StringUtils.isNotBlank(totalSalesParamsEntity.getGameNo())) {
                    response.put(GameSalesConstants.GAME_NO, totalSalesParamsEntity.getGameNo());
                    response.put(GameSalesConstants.CATEGORY_TOTAL_SALES, gameSalesRepository.sqlFunctionCalcTotalSalesForGameNoBetween(totalSalesParamsEntity.getFrom(), totalSalesParamsEntity.getTo(), Integer.valueOf(totalSalesParamsEntity.getGameNo())));

                } else {
                    response.put(GameSalesConstants.CATEGORY_TOTAL_SALES, gameSalesRepository.sqlFunctionCalcTotalSalesBetween(totalSalesParamsEntity.getFrom(), totalSalesParamsEntity.getTo()));
                }
            } else if (GameSalesConstants.CATEGORY_TOTAL_GAMES_COUNT.equals(totalSalesParamsEntity.getCategory())) {
                response.put(GameSalesConstants.CATEGORY_TOTAL_GAMES_COUNT, gameSalesRepository.sqlFunctionGetCountForTotalGamesSoldBetween(totalSalesParamsEntity.getFrom(), totalSalesParamsEntity.getTo()));
            }
        }
        return response;
    }

}
