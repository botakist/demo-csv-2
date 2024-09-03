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
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "invalid_record_seq")
    @SequenceGenerator(name = "invalid_record_seq", sequenceName = "invalid_record_seq")
    private Long id;
    @ManyToOne
    @JoinColumn(referencedColumnName = "id", nullable = false, foreignKey = @ForeignKey(name = "fk_progress_track_view_id"))
    private ProgressTrackingView progressTrackView;
    @Column(nullable = false)
    private Long invalidRecordRowId;
    @Column(nullable = false)
    private String invalidRecordRowText;
    @Column(nullable = false)
    private LocalDateTime createdOn;
}
