package com.wikistream.repository;

import com.wikistream.entity.WikimediaData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface WikimediaDataRepo extends JpaRepository<WikimediaData, Long> {

}
