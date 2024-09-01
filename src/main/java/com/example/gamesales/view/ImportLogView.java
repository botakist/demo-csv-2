package com.example.gamesales.view;

import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.Cache;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Getter
@Setter
@Table(name = "import_log")
@Cacheable
@Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
public class ImportLogView {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private Integer totalRecordsCount;
    private Integer totalProcessedRecordsCount;
    private Integer invalidRecordsCount;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private String status;
}
