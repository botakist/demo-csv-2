package com.example.gamesales.repository;

import com.example.gamesales.view.ImportLogView;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ImportLogRepository extends JpaRepository<ImportLogView, Long> {
}
