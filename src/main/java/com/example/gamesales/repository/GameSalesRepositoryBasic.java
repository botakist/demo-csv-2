package com.example.gamesales.repository;

import com.example.gamesales.view.GameSalesView;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Repository
public interface GameSalesRepositoryBasic extends JpaRepository<GameSalesView, BigDecimal> {
    @Query(value = "select count_game_sales_between(:from, :to)", nativeQuery = true)
    Long sqlFunctionGetCountForTotalGamesSoldBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);

    @Query(value = "select calc_total_sales_between(:from, :to)", nativeQuery = true)
    BigDecimal sqlFunctionCalcTotalSalesBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);

    @Query(value = "select calc_total_sales_between_for_game_no(:from, :to, :gameNo)", nativeQuery = true)
    BigDecimal sqlFunctionCalcTotalSalesForGameNoBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to, @Param("gameNo") Integer gameNo);
}
