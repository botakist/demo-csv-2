package com.example.gamesales.service;

import com.example.gamesales.exception.ValidationException;
import com.example.gamesales.repository.ProgressTrackingRepository;
import com.example.gamesales.view.ProgressTrackingView;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.text.MessageFormat;

@Service
@Slf4j
public class ProgressTrackingService {

    private final ProgressTrackingRepository progressTrackingRepository;

    @Autowired
    public ProgressTrackingService(ProgressTrackingRepository progressTrackingRepository) {
        this.progressTrackingRepository = progressTrackingRepository;
    }

    @Transactional
    public void updateProgress(ProgressTrackingView view) {
        ProgressTrackingView viewToUpdate = progressTrackingRepository.findById(view.getId()).orElseThrow(() -> {
            String unableToFindRecordWithId = MessageFormat.format("Unable to find view with id {0}", view.getId());
            return new ValidationException(unableToFindRecordWithId);
        });
        viewToUpdate.setTotalRecordsCount(view.getTotalRecordsCount());
        if (StringUtils.isNotBlank(view.getStatus())) {
            viewToUpdate.setStatus(view.getStatus());
        }

        if (view.getStartTime() != null) {
            viewToUpdate.setStartTime(view.getStartTime());
        }

        if (view.getEndTime() != null) {
            viewToUpdate.setEndTime(view.getEndTime());
        }

        if (view.getTotalProcessedRecordsCount() != null) {
            viewToUpdate.setTotalProcessedRecordsCount(view.getTotalProcessedRecordsCount());
        }

        if (view.getInvalidRecordsCount() != null) {
            viewToUpdate.setInvalidRecordsCount(view.getInvalidRecordsCount());
        }

        if (view.getTotalRecordsCount() != null) {
            viewToUpdate.setTotalRecordsCount(view.getTotalRecordsCount());
        }
        progressTrackingRepository.save(viewToUpdate);
    }
}
