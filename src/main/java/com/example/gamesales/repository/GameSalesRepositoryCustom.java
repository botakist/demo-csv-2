package com.example.gamesales.repository;

import com.example.gamesales.entity.GameSalesParamsEntity;
import com.example.gamesales.entity.TotalSalesParamsEntity;
import com.example.gamesales.view.GameSalesView;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.math.BigDecimal;

public interface GameSalesRepositoryCustom {
    Page<GameSalesView> getGameSalesWithPage(GameSalesParamsEntity params, Pageable pageable);
    Long getTotalGamesCountWith(TotalSalesParamsEntity params);
    BigDecimal getTotalSalesWith(TotalSalesParamsEntity params);
}
