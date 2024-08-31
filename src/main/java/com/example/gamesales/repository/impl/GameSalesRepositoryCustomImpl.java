package com.example.gamesales.repository.impl;

import com.example.gamesales.constants.GameSalesConstants;
import com.example.gamesales.entity.GameSalesParamsEntity;
import com.example.gamesales.entity.TotalSalesParamsEntity;
import com.example.gamesales.repository.GameSalesRepositoryCustom;
import com.example.gamesales.util.GameSalesUtil;
import com.example.gamesales.view.GameSalesView;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.query.QueryUtils;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class GameSalesRepositoryCustomImpl implements GameSalesRepositoryCustom {
    @PersistenceContext
    private EntityManager entityManager;

    @Override
    public Page<GameSalesView> getGameSalesWithPage(GameSalesParamsEntity params, Pageable pageable) {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();

        CriteriaQuery<GameSalesView> criteriaQuery = criteriaBuilder.createQuery(GameSalesView.class);

        Root<GameSalesView> criteriaRoot = criteriaQuery.from(GameSalesView.class);
        List<Predicate> predicates = new ArrayList<>();


        if (params.getFrom() != null) {
            predicates.add(criteriaBuilder.greaterThanOrEqualTo(criteriaRoot.get(GameSalesConstants.DATE_OF_SALE), GameSalesUtil.convertToDate(params.getFrom())));
        }

        if (params.getTo() != null) {
            predicates.add(criteriaBuilder.lessThanOrEqualTo(criteriaRoot.get(GameSalesConstants.DATE_OF_SALE), GameSalesUtil.convertToDate(params.getTo())));
        }

        if (params.getMaxPrice() != null) {
            predicates.add(criteriaBuilder.lessThanOrEqualTo(criteriaRoot.get(GameSalesConstants.COST_PRICE), params.getMaxPrice()));
        }

        if (params.getMinPrice() != null) {
            predicates.add(criteriaBuilder.greaterThanOrEqualTo(criteriaRoot.get(GameSalesConstants.COST_PRICE), params.getMinPrice()));
        }
        criteriaQuery.where(criteriaBuilder.and(predicates.toArray(new Predicate[predicates.size()])));

        if (pageable.getSort().isEmpty()) {
            criteriaQuery.orderBy(criteriaBuilder.desc(criteriaRoot.get(GameSalesConstants.DATE_OF_SALE)));
        } else {
            criteriaQuery.orderBy(QueryUtils.toOrders(pageable.getSort(), criteriaRoot, criteriaBuilder));
        }
        Query query = entityManager.createQuery(criteriaQuery);
        query.setFirstResult((int) pageable.getOffset());
        query.setMaxResults(pageable.getPageSize());
        List<GameSalesView> result = query.getResultList();

        CriteriaQuery<Long> countQuery = criteriaBuilder.createQuery(Long.class);
        Root<GameSalesView> countRoot = countQuery.from(GameSalesView.class);
        countQuery.select(criteriaBuilder.count(countRoot)).where(criteriaBuilder.and(predicates.toArray(new Predicate[predicates.size()])));
        Long count = entityManager.createQuery(countQuery).getSingleResult();

        return new PageImpl<>(result, pageable, count);
    }

    @Override
    public Long getTotalGamesCountWith(TotalSalesParamsEntity params) {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Long> countQuery = criteriaBuilder.createQuery(Long.class);
        Root<GameSalesView> totalGamesCountRoot = countQuery.from(GameSalesView.class);

        List<Predicate> predicates = new ArrayList<>();

        predicates.add(criteriaBuilder.greaterThanOrEqualTo(totalGamesCountRoot.get(GameSalesConstants.DATE_OF_SALE), GameSalesUtil.convertToDate(params.getFrom())));
        predicates.add(criteriaBuilder.lessThanOrEqualTo(totalGamesCountRoot.get(GameSalesConstants.DATE_OF_SALE), GameSalesUtil.convertToDate(params.getTo())));
        countQuery.select(criteriaBuilder.count(totalGamesCountRoot)).where(criteriaBuilder.and(predicates.toArray(new Predicate[predicates.size()])));
        return entityManager.createQuery(countQuery).getSingleResult();
    }

    @Override
    public BigDecimal getTotalSalesWith(TotalSalesParamsEntity params) {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BigDecimal> totalSalesQuery = criteriaBuilder.createQuery(BigDecimal.class);
        Root<GameSalesView> totalSalesRoot = totalSalesQuery.from(GameSalesView.class);
        List<Predicate> predicates = new ArrayList<>();

        predicates.add(criteriaBuilder.greaterThanOrEqualTo(totalSalesRoot.get(GameSalesConstants.DATE_OF_SALE), GameSalesUtil.convertToDate(params.getFrom())));
        predicates.add(criteriaBuilder.lessThanOrEqualTo(totalSalesRoot.get(GameSalesConstants.DATE_OF_SALE), GameSalesUtil.convertToDate(params.getTo())));
        if (StringUtils.isNotBlank(params.getGameNo())) {
            predicates.add(criteriaBuilder.equal(totalSalesRoot.get(GameSalesConstants.GAME_NO), params.getGameNo()));
        }
        totalSalesQuery.where(criteriaBuilder.and(predicates.toArray(new Predicate[predicates.size()])));
        totalSalesQuery.select(criteriaBuilder.sum(totalSalesRoot.get(GameSalesConstants.SALE_PRICE)));
        return entityManager.createQuery(totalSalesQuery).getSingleResult();
    }
}
