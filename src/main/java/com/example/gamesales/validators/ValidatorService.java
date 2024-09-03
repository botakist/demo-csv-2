package com.example.gamesales.validators;

import com.example.gamesales.constants.GameSalesConstants;
import com.example.gamesales.entity.GameSalesParamsEntity;
import com.example.gamesales.entity.TotalSalesParamsEntity;
import com.example.gamesales.exception.ValidationException;
import com.example.gamesales.util.GameSalesUtil;
import com.example.gamesales.view.GameSalesView;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.compare.ComparableUtils;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.Objects;

@Service
@Slf4j
public class ValidatorService {
    private static final ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

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

    public void logAndThrowValidationException(String errorMessage) {
        log.error(errorMessage);
        throw new ValidationException(errorMessage);
    }


    public boolean isValidData(GameSalesView view) {
        boolean gameNo = view.getGameNo() > 0;
        boolean gameName = StringUtils.isNotBlank(view.getGameName()) && view.getGameName().length() <= 20;
        boolean gameCode = StringUtils.isNotBlank(view.getGameCode()) && view.getGameCode().length() <= 5;
        boolean type = view.getType() == 1 || view.getType() == 2;
        boolean costPrice = view.getCostPrice() >= 0 && view.getCostPrice() <= 100;
        boolean tax = view.getTax() >= 0;
        boolean salePrice = view.getSalePrice() >= 0;
        boolean dateOfSale = view.getDateOfSale() != null;
        return gameNo && gameName && gameCode && type && costPrice && tax && salePrice && dateOfSale;
    }
}
