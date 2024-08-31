package com.example.gamesales.view;

import com.example.gamesales.serializer.CustomLocalDateTimeSerializer;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.time.LocalDateTime;

@Entity
@Getter
@Setter
@Table(name = "game_sales")
public class GameSalesView {
    @Id
    private Long id;
    private int gameNo;
    private String gameName;
    private String gameCode;
    private int type;
    private Double costPrice;
    private Double tax;
    private Double salePrice;
    @JsonSerialize(using = CustomLocalDateTimeSerializer.class)
    private LocalDateTime dateOfSale;
}
