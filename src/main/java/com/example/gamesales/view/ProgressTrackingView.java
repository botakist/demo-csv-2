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
@Table(name = "progress_tracking")
@Cacheable
@Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
public class ProgressTrackingView {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "prog_trk_seq")
    @SequenceGenerator(name = "prog_trk_seq", sequenceName = "prog_trk_seq", allocationSize = 5000)
    private Long id;
    private Integer totalRecordsCount;
    private Integer totalProcessedRecordsCount;
    private Integer invalidRecordsCount;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private String status;
}
