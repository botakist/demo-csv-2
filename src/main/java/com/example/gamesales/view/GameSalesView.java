package com.example.gamesales.view;

import com.example.gamesales.serializer.CustomLocalDateTimeSerializer;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Getter
@Setter
@Table(name = "game_sales",
        indexes = {
                @Index(columnList = "gameNo"),
                @Index(columnList = "dateOfSale DESC"),
                @Index(columnList = "dateOfSale DESC, salePrice"),
                @Index(columnList = "dateOfSale DESC, gameNo, salePrice")
        }
)
public class GameSalesView {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE)
    private Long id;
    @Column(nullable = false)
    private int gameNo;
    @Column(nullable = false)
    private String gameName;
    @Column(nullable = false)
    private String gameCode;
    @Column(nullable = false)
    private int type;
    @Column(nullable = false)
    private Double costPrice;
    @Column(nullable = false)
    private Double tax;
    @Column(nullable = false)
    private Double salePrice;
    @JsonSerialize(using = CustomLocalDateTimeSerializer.class)
    @Column(nullable = false)
    private LocalDateTime dateOfSale;
}
