package com.example.gamesales.view;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Getter
@Setter
@Table(name = "invalid_record")
public class InvalidRecordView {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "invalid_rec_seq")
    @SequenceGenerator(name = "invalid_rec_seq", sequenceName = "invalid_rec_seq", allocationSize = 5000)
    private Long id;
    private Long importLogId;
    @Column(nullable = false)
    private Long invalidRecordRowId;
    @Column(nullable = false)
    private String invalidRecordRowText;
    @Column(nullable = false)
    private LocalDateTime createdOn;
}
