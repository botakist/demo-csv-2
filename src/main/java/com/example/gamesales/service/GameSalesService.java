package com.example.gamesales.service;

import com.example.gamesales.constants.GameSalesConstants;
import com.example.gamesales.entity.GameSalesParamsEntity;
import com.example.gamesales.entity.TotalSalesParamsEntity;
import com.example.gamesales.exception.ValidationException;
import com.example.gamesales.repository.GameSalesRepository;
import com.example.gamesales.repository.ImportLogRepository;
import com.example.gamesales.util.GameSalesUtil;
import com.example.gamesales.view.GameSalesView;
import com.example.gamesales.view.ImportLogView;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.compare.ComparableUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.transaction.Transactional;
import java.io.IOException;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
@Slf4j
public class GameSalesService {
    private static final ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final GameSalesRepository gameSalesRepository;
    private final ImportLogRepository importLogRepository;

    private final JdbcTemplate jdbcTemplate;

    @Value("${com.example.gamesales.import.threadpoolsize:20}")
    private int threadPoolSize;

    @Value("${spring.jpa.properties.hibernate.jdbc.batch_size:10000}")
    private int batchSize;

    public GameSalesService(GameSalesRepository gameSalesRepository, ImportLogRepository importLogRepository, JdbcTemplate jdbcTemplate) {
        this.gameSalesRepository = gameSalesRepository;
        this.importLogRepository = importLogRepository;
        this.jdbcTemplate = jdbcTemplate;
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
        List<GameSalesView> gameSalesViews = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);

        ImportLogView importLogView = new ImportLogView();
        importLogView.setTotalRecordsCount(totalRecordsCount);
        importLogView.setStartTime(LocalDateTime.now());
        importLogView.setTotalProcessedRecordsCount(0);
        importLogView.setInvalidRecordsCount(0);
        importLogView.setStatus("IN_PROGRESS");
        importLogRepository.save(importLogView);

        AtomicInteger validRecordsCount = new AtomicInteger();
        AtomicInteger invalidRecordsCount = new AtomicInteger();

        try (LineIterator it = FileUtils.lineIterator(GameSalesUtil.multipartToFile(csvFile), "UTF-8")) {
            // skip csv file header line
            if (it.hasNext()) {
                it.next();
            }

            while (it.hasNext()) {
                String line = it.nextLine();
                line = line.replace("\"", "");
                String[] fields = line.split(",");
                try {
                    gameSalesViews.add(parseCsvLineToGameSalesView(fields));

                } catch (NullPointerException | NumberFormatException | DateTimeParseException e) {
                    invalidRecordsCount.getAndIncrement();
                    importLogView.setInvalidRecordsCount(invalidRecordsCount.get());
                    updateImportLog(importLogView);
                    continue;
                }

                if (gameSalesViews.size() >= batchSize) {
                    List<GameSalesView> batch = new ArrayList<>(gameSalesViews);
                    executorService.submit(() -> validateAndBatchInsert(batch, invalidRecordsCount, validRecordsCount));
                    importLogView.setTotalProcessedRecordsCount(validRecordsCount.get());
                    importLogView.setInvalidRecordsCount(invalidRecordsCount.get());
                    updateImportLog(importLogView);
                    gameSalesViews.clear();
                }
            }
            if (!gameSalesViews.isEmpty()) {
                executorService.submit(() -> validateAndBatchInsert(gameSalesViews, invalidRecordsCount, validRecordsCount));
                importLogView.setTotalProcessedRecordsCount(validRecordsCount.get());
                importLogView.setInvalidRecordsCount(invalidRecordsCount.get());
                updateImportLog(importLogView);
            }
            importLogView.setStatus("COMPLETED");
            importLogView.setEndTime(LocalDateTime.now());
            updateImportLog(importLogView);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        } finally {
            executorService.shutdown();
        }
    }


    private void validateAndBatchInsert(List<GameSalesView> unvalidatedBatch, AtomicInteger invalidRecordsCount, AtomicInteger validRecordsCount) {
        List<GameSalesView> validatedBatch = new ArrayList<>();

        for (GameSalesView view : unvalidatedBatch) {
            if (isValidData(view)) {
                validatedBatch.add(view);
                validRecordsCount.getAndIncrement();
            } else {
                invalidRecordsCount.getAndIncrement();
            }
        }
        log.info(Thread.currentThread().getName());
        batchInsert(validatedBatch);
    }

    private boolean isValidData(GameSalesView view) {
        boolean id = view.getId() != null && view.getId() > 0;
        boolean gameNo = view.getGameNo() > 0;
        boolean gameName = StringUtils.isNotBlank(view.getGameName()) && view.getGameName().length() <= 20;
        boolean gameCode = StringUtils.isNotBlank(view.getGameCode()) && view.getGameCode().length() <= 5;
        boolean type = view.getType() == 1 || view.getType() == 2;
        boolean costPrice = view.getCostPrice() >= 0 && view.getCostPrice() <= 100;
        boolean tax = view.getTax() >= 0;
        boolean salePrice = view.getSalePrice() >= 0 && (view.getSalePrice() == (view.getCostPrice() + view.getCostPrice() * view.getTax()));
        boolean dateOfSale = view.getDateOfSale() != null;
        return id && gameNo && gameName && gameCode && type && costPrice && tax && salePrice && dateOfSale;
    }

    private GameSalesView parseCsvLineToGameSalesView(String[] fields) throws NullPointerException, NumberFormatException, DateTimeParseException {
        GameSalesView view = new GameSalesView();
        view.setId(Long.parseLong(fields[0]));
        view.setGameNo(Integer.parseInt(fields[1]));
        view.setGameName(fields[2]);
        view.setGameCode(fields[3]);
        view.setType(Integer.parseInt(fields[4]));
        view.setCostPrice(Double.parseDouble(fields[5]));
        view.setTax(Double.parseDouble(fields[6]));
        view.setSalePrice(Double.parseDouble(fields[7]));
        view.setDateOfSale(LocalDateTime.parse(fields[8], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")));
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

    public void batchInsert(List<GameSalesView> views) {
        String sql = "INSERT INTO game_sales (id, game_no, game_name, game_code, type, cost_price, tax, sale_price, date_of_sale) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        List<Object[]> batchArgs = views.stream()
                .map(view -> new Object[]{
                        view.getId(),
                        view.getGameNo(),
                        view.getGameName(),
                        view.getGameCode(),
                        view.getType(),
                        view.getCostPrice(),
                        view.getTax(),
                        view.getSalePrice(),
                        view.getDateOfSale()
                })
                .collect(Collectors.toList());
        jdbcTemplate.batchUpdate(sql, batchArgs);
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

    private void updateImportLog(ImportLogView view) {
        ImportLogView viewToUpdate = importLogRepository.findById(view.getId()).orElseThrow(() -> {
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
        importLogRepository.save(viewToUpdate);
    }
}
