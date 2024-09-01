package com.example.gamesales.service;

import com.example.gamesales.exception.ValidationException;
import com.example.gamesales.view.GameSalesView;
import com.example.gamesales.view.InvalidRecordView;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class BatchInsertService {
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public BatchInsertService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public void batchInsertGameSales(List<GameSalesView> views) {
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
        try {
            jdbcTemplate.batchUpdate(sql, batchArgs);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new ValidationException(e.getMessage());
        }
    }

    @Transactional
    public void batchInsertInvalidRecords(List<InvalidRecordView> views) {
        String sql = "INSERT INTO invalid_record (invalid_record_row_id, invalid_record_row_text, created_on) VALUES (?, ?, ?)";
        List<Object[]> batchArgs = views.stream()
                .map(view -> new Object[]{
                        view.getInvalidRecordRowId(),
                        view.getInvalidRecordRowText(),
                        view.getCreatedOn()
                })
                .collect(Collectors.toList());
        try {
            jdbcTemplate.batchUpdate(sql, batchArgs);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new ValidationException(e.getMessage());
        }
    }
}
