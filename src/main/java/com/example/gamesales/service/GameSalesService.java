package com.example.gamesales.service;

import com.example.gamesales.constants.GameSalesConstants;
import com.example.gamesales.entity.GameSalesParamsEntity;
import com.example.gamesales.entity.TotalSalesParamsEntity;
import com.example.gamesales.exception.ValidationException;
import com.example.gamesales.repository.GameSalesRepository;
import com.example.gamesales.repository.ProgressTrackingRepository;
import com.example.gamesales.task.BatchInsertGameSalesTaskExecutor;
import com.example.gamesales.task.BatchInsertInvalidRecordsTaskExecutor;
import com.example.gamesales.util.GameSalesUtil;
import com.example.gamesales.view.GameSalesView;
import com.example.gamesales.view.InvalidRecordView;
import com.example.gamesales.view.ProgressTrackingView;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.compare.ComparableUtils;
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
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class GameSalesService {
    private static final ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final GameSalesRepository gameSalesRepository;
    private final ProgressTrackingRepository progressTrackingRepository;
    private final ExecutorService executorService;
    private final JdbcTemplate jdbcTemplate;
    private final BatchInsertService batchInsertService;
    private final ProgressTrackingService progressTrackingService;

    @Value("${com.example.gamesales.import.threadpoolsize:20}")
    private int threadPoolSize;

    @Value("${spring.jpa.properties.hibernate.jdbc.batch_size:10000}")
    private int batchSize;

    @Autowired
    public GameSalesService(GameSalesRepository gameSalesRepository, ProgressTrackingRepository progressTrackingRepository, ExecutorService executorService, JdbcTemplate jdbcTemplate, BatchInsertService batchInsertService, ProgressTrackingService progressTrackingService) {
        this.gameSalesRepository = gameSalesRepository;
        this.progressTrackingRepository = progressTrackingRepository;
        this.executorService = executorService;
        this.jdbcTemplate = jdbcTemplate;
        this.batchInsertService = batchInsertService;
        this.progressTrackingService = progressTrackingService;
    }

    public int validateCsvFile(MultipartFile csvFile) {
        if (Objects.isNull(csvFile) || csvFile.isEmpty()) {
            logAndThrowValidationException("csv file is empty.");

        }
        if (!StringUtils.endsWithIgnoreCase(csvFile.getOriginalFilename(), ".csv")) {
            logAndThrowValidationException("file input extension is not .csv");
        }

        int totalRecordsCount = 0;
        try {
            totalRecordsCount = GameSalesUtil.getTotalRecordsInside(csvFile);
            if (totalRecordsCount <= 0) {
                logAndThrowValidationException("csv file contains 0 records");
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            logAndThrowValidationException("error when getting total records count in csv file.");
        }
        return totalRecordsCount;
    }

    public void save(MultipartFile csvFile, int totalRecordsCount) {

        AtomicInteger validRecordsCount = new AtomicInteger();
        AtomicInteger invalidRecordsCount = new AtomicInteger();

        ProgressTrackingView progressTrackingView = initialiseProgressView(totalRecordsCount);

        List<GameSalesView> validGameSalesViews = new ArrayList<>();
        List<CSVRecord> invalidGameSalesRecords = new ArrayList<>();
        try (BufferedReader fileReader = new BufferedReader(new InputStreamReader(csvFile.getInputStream(), StandardCharsets.UTF_8));
             CSVParser csvParser = new CSVParser(fileReader, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim())) {
            Iterable<CSVRecord> csvRecords = csvParser.getRecords();

            for (CSVRecord csvRecord : csvRecords) {
                try {
                    // separate valid and invalid records first.
                    GameSalesView view = parseCsvLineToGameSalesView(csvRecord);
                    if (isValidData(view)) {
                        validGameSalesViews.add(view);
                    } else {
                        invalidGameSalesRecords.add(csvRecord);
                    }
                } catch (NullPointerException | NumberFormatException | DateTimeParseException e) {
                    log.error(e.getMessage(), e);
                    throw new ValidationException("error in parsing data in csv file.");
                }
            }
            // use JPA repository (very slow)
//            gameSalesRepository.saveAll(validGameSalesViews);

            // use jdbcTemplate batch insertion single threaded (still very slow)
//            int totalCount = validGameSalesViews.size();
//            for (int i = 0; i < totalCount; i += batchSize) {
//                int endIndex = Math.min(i + batchSize, totalCount);
//                // get sublist for current batch
//                List<GameSalesView> batchList = validGameSalesViews.subList(i, endIndex);
//                batchInsertService.batchInsertGameSales(batchList);
//                validRecordsCount.addAndGet(batchList.size());
//            }

            // use jdbcTemplate batch insertion multi-threaded txn
            BatchInsertGameSalesTaskExecutor executor = new BatchInsertGameSalesTaskExecutor(executorService, batchInsertService, progressTrackingService);
            executor.executeBatchGameSalesInserts(validGameSalesViews, batchSize, validRecordsCount, progressTrackingView);

            BatchInsertInvalidRecordsTaskExecutor executor2 = new BatchInsertInvalidRecordsTaskExecutor(executorService, batchInsertService);
            executor2.executeBatchInvalidRecordInserts(mapToInvalidRecordViews(invalidGameSalesRecords), batchSize, invalidRecordsCount);

        } catch (IOException e) {
            log.error(e.getMessage(), e);
            progressTrackingView.setStatus("ERROR");
            progressTrackingView.setEndTime(LocalDateTime.now());
            updateProgress(progressTrackingView);
            throw new ValidationException("error reading data from csv file.");
        } finally {
            executorService.shutdown();
            // commenting this part returns the response faster while the app still runs the batch inserts in the background.
//            try {
//                executorService.awaitTermination(1, TimeUnit.MINUTES);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } finally {
//                executorService.shutdownNow();
//            }
        }
    }

    private ProgressTrackingView initialiseProgressView(int totalRecordsCount) {
        ProgressTrackingView progressTrackingView = new ProgressTrackingView();
        progressTrackingView.setTotalRecordsCount(totalRecordsCount);
        progressTrackingView.setStartTime(LocalDateTime.now());
        progressTrackingView.setTotalProcessedRecordsCount(0);
        progressTrackingView.setInvalidRecordsCount(0);
        progressTrackingView.setStatus("IN_PROGRESS");
        progressTrackingRepository.save(progressTrackingView);
        return progressTrackingView;
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

    private boolean isValidData(GameSalesView view) {
        boolean id = view.getId() != null && view.getId() > 0;
        boolean gameNo = view.getGameNo() > 0;
        boolean gameName = StringUtils.isNotBlank(view.getGameName()) && view.getGameName().length() <= 20;
        boolean gameCode = StringUtils.isNotBlank(view.getGameCode()) && view.getGameCode().length() <= 5;
        boolean type = view.getType() == 1 || view.getType() == 2;
        boolean costPrice = view.getCostPrice() >= 0 && view.getCostPrice() <= 100;
        boolean tax = view.getTax() >= 0;
        boolean salePrice = view.getSalePrice() >= 0;
        boolean dateOfSale = view.getDateOfSale() != null;
        return id && gameNo && gameName && gameCode && type && costPrice && tax && salePrice && dateOfSale;
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

    public GameSalesParamsEntity validateGetGameSalesRequest(String params, String sortField, String sortDir) {
        GameSalesParamsEntity gameSalesParamsEntity = null;
        if (StringUtils.isNotBlank(params)) {
            try {
                gameSalesParamsEntity = mapper.readValue(params, GameSalesParamsEntity.class);
            } catch (JsonProcessingException e) {
                log.error(e.getMessage(), e);
                throw new ValidationException(GameSalesConstants.PARAMS_MAPPING_ERROR_ENCOUNTERED_CONTACT_ADMIN);
            }
            if (ObjectUtils.isEmpty(gameSalesParamsEntity)) {
                log.error("gameSalesParamsEntity is null");
                throw new ValidationException(GameSalesConstants.PARAMS_MAPPING_ERROR_ENCOUNTERED_CONTACT_ADMIN);
            }
            validateFromDateCannotBeAfterToDateNonMandatory(gameSalesParamsEntity.getFrom(), gameSalesParamsEntity.getTo());
            validateMinPriceMaxPriceNonMandatory(gameSalesParamsEntity.getMinPrice(), gameSalesParamsEntity.getMaxPrice());
        }
        validateValidSortFields(sortField);
        validateValidSortDirection(sortDir);

        return gameSalesParamsEntity;
    }

    public TotalSalesParamsEntity validateTotalSalesRequest(String params) {
        TotalSalesParamsEntity totalSalesParamsEntity;
        validateParamsNotEmpty(params);
        try {
            totalSalesParamsEntity = mapper.readValue(params, TotalSalesParamsEntity.class);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage(), e);
            throw new ValidationException(GameSalesConstants.PARAMS_MAPPING_ERROR_ENCOUNTERED_CONTACT_ADMIN);
        }

        if (ObjectUtils.anyNull(totalSalesParamsEntity, totalSalesParamsEntity.getFrom(), totalSalesParamsEntity.getTo())) {
            String mandatoryParamsEmpty = "mandatory parameters 'from' or 'to' are empty";
            log.error(mandatoryParamsEmpty);
            throw new ValidationException(mandatoryParamsEmpty);
        }
        validateFromDateCannotBeAfterToDateNonMandatory(totalSalesParamsEntity.getFrom(), totalSalesParamsEntity.getTo());
        validateCategoryAndGameNo(totalSalesParamsEntity.getCategory(), totalSalesParamsEntity.getGameNo());
        return totalSalesParamsEntity;
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


    private void validateCategoryAndGameNo(String category, String gameNo) {
        if (StringUtils.isBlank(category)) {
            String categoryIsEmpty = "Category is empty";
            log.error(categoryIsEmpty);
            throw new ValidationException(categoryIsEmpty);
        }

        if (!StringUtils.containsAnyIgnoreCase(category, GameSalesConstants.CATEGORY_TOTAL_GAMES_COUNT, GameSalesConstants.CATEGORY_TOTAL_SALES)) {
            String invalidCategory = MessageFormat.format("Invalid category \"{0}\". Category must be either \"{1}\" or \"{2}\"", category, GameSalesConstants.CATEGORY_TOTAL_GAMES_COUNT, GameSalesConstants.CATEGORY_TOTAL_SALES);
            log.error(invalidCategory);
            throw new ValidationException(invalidCategory);
        }

        if (StringUtils.isNotBlank(gameNo)) {
            int gameNoInt = Integer.parseInt(gameNo);
            if (gameNoInt < 0 || gameNoInt > 101) {
                String invalidGameNo = "GameNo must be between 1 and 100";
                log.error(invalidGameNo);
                throw new ValidationException(invalidGameNo);
            }
        }
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

    public void validateFromDateCannotBeAfterToDateNonMandatory(LocalDateTime from, LocalDateTime to) {
        if (ObjectUtils.allNotNull(from, to) && from.isAfter(to)) {
            String fromDateIsAfterToDate = MessageFormat.format("From:{0} cannot be after To:{1}", from, to);
            logAndThrowValidationException(fromDateIsAfterToDate);
        }
    }

    public void validateMinPriceMaxPriceNonMandatory(Double minPrice, Double maxPrice) {
        if ((minPrice != null && ComparableUtils.is(minPrice).lessThan(0.0))) {
            String invalidMinPrice = MessageFormat.format("Invalid param minPrice:{0}, minPrice cannot be null or less than 0.0", minPrice);
            logAndThrowValidationException(invalidMinPrice);
        }
        if ((maxPrice != null && ComparableUtils.is(maxPrice).lessThan(0.0))) {
            String invalidMaxPrice = MessageFormat.format("Invalid param maxPrice:{0}, maxPrice cannot be null or less than 0.0", maxPrice);
            logAndThrowValidationException(invalidMaxPrice);
        }

        if (ObjectUtils.allNotNull(minPrice, maxPrice) && ComparableUtils.is(minPrice).greaterThan(maxPrice)) {
            String minPriceIsGreaterThanMaxPrice = MessageFormat.format("minPrice {0} cannot be greater than maxPrice {1}", minPrice, maxPrice);
            logAndThrowValidationException(minPriceIsGreaterThanMaxPrice);
        }
    }

    public void validateParamsNotEmpty(String params) {
        if (StringUtils.isEmpty(params)) {
            String paramsIsEmpty = "params is empty";
            logAndThrowValidationException(paramsIsEmpty);
        }
    }

    private void validateValidSortDirection(String sortDir) {
        if (!StringUtils.equalsAnyIgnoreCase(sortDir, GameSalesConstants.SORT_DIR_ASC, GameSalesConstants.SORT_DIR_DESC)) {
            String invalidSortDirection = MessageFormat.format("parameter sortDir:{0} is invalid. It should be either {1} or {2}", sortDir, GameSalesConstants.SORT_DIR_ASC, GameSalesConstants.SORT_DIR_DESC);
            logAndThrowValidationException(invalidSortDirection);
        }
    }

    private void validateValidSortFields(String sortField) {
        String[] validColumnFieldsName = {"id", "game_no", "game_name", "game_code", "type", "cost_price", "tax", "sale_price", "date_of_sale"};
        if (!GameSalesUtil.isStringInArrayIgnoreCase(sortField, validColumnFieldsName)) {
            String invalidSortFields = MessageFormat.format("parameter sortField:{0} is not a valid field in game_sales table.", sortField);
            logAndThrowValidationException(invalidSortFields);
        }
    }

    private void logAndThrowValidationException(String errorMessage) {
        log.error(errorMessage);
        throw new ValidationException(errorMessage);
    }

    private void updateProgress(ProgressTrackingView view) {
        ProgressTrackingView viewToUpdate = progressTrackingRepository.findById(view.getId()).orElseThrow(() -> {
            String unableToFindRecordWithId = MessageFormat.format("Unable to find view with id {0}", view.getId());
            return new ValidationException(unableToFindRecordWithId);
        });
        viewToUpdate.setTotalRecordsCount(view.getTotalRecordsCount());
        if (StringUtils.isNotBlank(view.getStatus())) {
            viewToUpdate.setStatus(view.getStatus());
        }

        if (view.getStartTime() != null) {
            viewToUpdate.setStartTime(view.getStartTime());
        }

        if (view.getEndTime() != null) {
            viewToUpdate.setEndTime(view.getEndTime());
        }

        if (view.getTotalProcessedRecordsCount() != null) {
            viewToUpdate.setTotalProcessedRecordsCount(view.getTotalProcessedRecordsCount());
        }

        if (view.getInvalidRecordsCount() != null) {
            viewToUpdate.setInvalidRecordsCount(view.getInvalidRecordsCount());
        }

        if (view.getTotalRecordsCount() != null) {
            viewToUpdate.setTotalRecordsCount(view.getTotalRecordsCount());
        }
        progressTrackingRepository.save(viewToUpdate);
    }
}
