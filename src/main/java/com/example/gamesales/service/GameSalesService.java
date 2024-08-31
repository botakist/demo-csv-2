package com.example.gamesales.service;

import com.example.gamesales.constants.GameSalesConstants;
import com.example.gamesales.entity.GameSalesParamsEntity;
import com.example.gamesales.entity.TotalSalesParamsEntity;
import com.example.gamesales.exception.ValidationException;
import com.example.gamesales.repository.GameSalesRepository;
import com.example.gamesales.util.GameSalesUtil;
import com.example.gamesales.view.GameSalesView;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.compare.ComparableUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.transaction.Transactional;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Service
@Slf4j
public class GameSalesService {
    private static final ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final GameSalesRepository gameSalesRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${com.example.gamesales.import.threadpoolsize:20}")
    private int threadPoolSize;

    private static final int batchSize = 10000;

    public GameSalesService(GameSalesRepository gameSalesRepository) {
        this.gameSalesRepository = gameSalesRepository;
    }

    public String validateCsvFile(MultipartFile csvFile) {
        if (Objects.isNull(csvFile) || csvFile.isEmpty()) {
            return "file is empty";
        }

        if (!StringUtils.endsWithIgnoreCase(csvFile.getOriginalFilename(), ".csv")) {
            return "input file is not a .csv file";
        }
        return null;
    }

    @Transactional
    public void save(MultipartFile csvFile) {
        List<GameSalesView> gameSalesViews = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);
        try (LineIterator it = FileUtils.lineIterator(GameSalesUtil.multipartToFile(csvFile), "UTF-8")) {
            if (it.hasNext()) {
                it.next();
            }
            while (it.hasNext()) {
                String line = it.nextLine();
                line = line.replace("\"", "");
                String[] fields = line.split(",");
                GameSalesView view = new GameSalesView();
                view.setId(Long.parseLong(fields[0]));
                view.setGameNo(Integer.parseInt(fields[1]));
                view.setGameName(fields[2]);
                view.setGameCode(fields[3]);
                view.setType(Integer.parseInt(fields[4]));
                view.setCostPrice(Double.parseDouble(fields[5]));
                view.setTax(Double.parseDouble(fields[6]));
                view.setSalePrice(Double.parseDouble(fields[7]));
                view.setDateOfSale(LocalDateTime.parse(fields[8],  DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")));
                gameSalesViews.add(view);

                if (gameSalesViews.size() <= batchSize) {
                    List<GameSalesView> batch = new ArrayList<>(gameSalesViews);
                    executorService.submit(() -> batchInsert(batch));
                    gameSalesViews.clear();
                }
            }
            if (!gameSalesViews.isEmpty()) {
                executorService.submit(() -> batchInsert(gameSalesViews));
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        } finally {
            executorService.shutdown();
        }
    }

    public GameSalesParamsEntity validateGetGameSalesRequest(String params, String sortField, String sortDir) {
        GameSalesParamsEntity gameSalesParamsEntity = new GameSalesParamsEntity();
        validateParamsNotEmpty(params);
        try {
            gameSalesParamsEntity = mapper.readValue(params, GameSalesParamsEntity.class);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new ValidationException("params mapping error encountered, contact admin.");
        }
        if (gameSalesParamsEntity == null) {
            log.error("gameSalesParamsEntity is null");
        }
        validateFromAndToDates(gameSalesParamsEntity.getFrom(), gameSalesParamsEntity.getTo());
        validateMinAndMaxPrice(gameSalesParamsEntity.getMinPrice(), gameSalesParamsEntity.getMaxPrice());

        return gameSalesParamsEntity;
    }


    public TotalSalesParamsEntity validateTotalSalesRequest(String params) {
        TotalSalesParamsEntity totalSalesParamsEntity;
        validateParamsNotEmpty(params);
        try {
            totalSalesParamsEntity = mapper.readValue(params, TotalSalesParamsEntity.class);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new ValidationException("params mapping error encountered, contact admin.");
        }

        if (ObjectUtils.anyNull(totalSalesParamsEntity, totalSalesParamsEntity.getFrom(), totalSalesParamsEntity.getTo())) {
            String mandatoryParamsEmpty = "mandatory parameters 'from' or 'to' are empty";
            log.error(mandatoryParamsEmpty);
            throw new ValidationException(mandatoryParamsEmpty);
        }
        validateFromAndToDates(totalSalesParamsEntity.getFrom(), totalSalesParamsEntity.getTo());
        validateCategoryAndGameNo(totalSalesParamsEntity.getCategory(), totalSalesParamsEntity.getGameNo());
        return totalSalesParamsEntity;
    }


    public Page<GameSalesView> getGameSalesWith(GameSalesParamsEntity gameSalesParamsEntity, String sortField, String sortDir, Pageable pageable) {
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

        if (StringUtils.isNotBlank(sortField)) {
            sql.append(" ORDER BY ").append(sortField);
        } else {
            sql.append(" ORDER BY date_of_sale");
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

    public void validateFromAndToDates(LocalDateTime from, LocalDateTime to) {
        if (ObjectUtils.allNotNull(from, to) && from.isAfter(to)) {
            String fromDateIsAfterToDate = MessageFormat.format("From:{0} cannot be after To:{1}", from, to);
            log.error(fromDateIsAfterToDate);
            throw new ValidationException(fromDateIsAfterToDate);
        }
    }

    public void validateMinAndMaxPrice(Double minPrice, Double maxPrice) {
        if (ObjectUtils.allNotNull(minPrice, maxPrice) && ComparableUtils.is(minPrice).greaterThan(maxPrice)) {
            String minPriceIsGreaterThanMaxPrice = MessageFormat.format("minPrice {0} cannot be greater than maxPrice {1}", minPrice, maxPrice);
            log.error(minPriceIsGreaterThanMaxPrice);
            throw new ValidationException(minPriceIsGreaterThanMaxPrice);
        }
    }

    public void validateParamsNotEmpty(String params) {
        if (StringUtils.isEmpty(params)) {
            String paramsIsEmpty = "params is empty";
            log.error(paramsIsEmpty);
            throw new ValidationException(paramsIsEmpty);
        }
    }

    public void batchInsert(List<GameSalesView> views) {
        String sql = "INSERT INTO game_sales (id, game_no, game_name, game_code, type, cost_price, tax, sale_price, date_of_sale) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                GameSalesView entity = views.get(i);
                ps.setString(1, String.valueOf(entity.getId()));
                ps.setString(2, String.valueOf(entity.getGameNo()));
                ps.setString(3, entity.getGameName());
                ps.setString(4, entity.getGameCode());
                ps.setString(5, String.valueOf(entity.getType()));
                ps.setString(6, String.valueOf(entity.getCostPrice()));
                ps.setString(7, String.valueOf(entity.getTax()));
                ps.setString(8, String.valueOf(entity.getSalePrice()));
                ps.setString(9, String.valueOf(entity.getDateOfSale()));
            }
            @Override
            public int getBatchSize() {
                return views.size();
            }
        });
    }
}
