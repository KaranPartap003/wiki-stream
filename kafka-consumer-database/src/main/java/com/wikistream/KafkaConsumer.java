package com.wikistream;

import com.wikistream.entity.WikimediaData;
import com.wikistream.repository.WikimediaDataRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

    @Autowired
    private WikimediaDataRepo dataRepository;

    @KafkaListener(topics = "wikimedia-recentchange", groupId = "myGroup")
    public void consume(String eventMessage){
        LOGGER.info(String.format("message received by consumer: %s", eventMessage));
        WikimediaData wikiData = new WikimediaData();
        wikiData.setWikiEventData(eventMessage.substring(0,255));
        dataRepository.save(wikiData);
    }
}
