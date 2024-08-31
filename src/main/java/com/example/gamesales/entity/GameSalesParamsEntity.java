package com.example.gamesales.entity;

import com.example.gamesales.deserializer.CustomLocalDateTimeDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Getter
@Setter
@ToString
public class GameSalesParamsEntity {
    @JsonDeserialize(using = CustomLocalDateTimeDeserializer.class)
    private LocalDateTime from;
    @JsonDeserialize(using = CustomLocalDateTimeDeserializer.class)
    private LocalDateTime to;
    private Double minPrice;
    private Double maxPrice;


    public boolean isEmpty() {
        return from == null && to == null && minPrice == null && maxPrice == null;
    }
}
