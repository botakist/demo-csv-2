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
@Table(name = "game_sales"
        , indexes = {
        @Index(name = "idx_date_of_sale", columnList = "dateOfSale"),
        @Index(name = "idx_date_of_sale_sale_price", columnList = "dateOfSale, salePrice"),
        @Index(name = "idx_date_of_sale_sale_price_game_no", columnList = "dateOfSale, salePrice, gameNo")
})
public class GameSalesView {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "game_sales_seq")
    @SequenceGenerator(name = "game_sales_seq", sequenceName = "game_sales_seq")
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
