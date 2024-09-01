package com.example.gamesales.entity;

import com.example.gamesales.deserializer.CustomLocalDateTimeDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.LocalDateTime;

@Getter
@Setter
@ToString
public class TotalSalesParamsEntity {
    @JsonDeserialize(using = CustomLocalDateTimeDeserializer.class)
    private LocalDateTime from;
    @JsonDeserialize(using = CustomLocalDateTimeDeserializer.class)
    private LocalDateTime to;
    private String category;
    private String gameNo;
}
