package com.example.gamesales.repository;

import com.example.gamesales.view.ProgressTrackingView;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProgressTrackingRepository extends JpaRepository<ProgressTrackingView, Long> {
}
