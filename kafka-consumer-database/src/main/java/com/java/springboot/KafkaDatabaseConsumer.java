package com.java.springboot;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.java.springboot.entity.WikimediaData;
import com.java.springboot.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDatabaseConsumer.class);


    private final WikimediaDataRepository dataRepository;

    private final ObjectMapper objectMapper = new ObjectMapper(); // Used for JSON processing


    public KafkaDatabaseConsumer(WikimediaDataRepository dataRepository) {
        this.dataRepository = dataRepository;
    }

    @KafkaListener(topics = "wikimedia_recentchange",
            groupId = "myGroup")
    public void consume(String eventMessage) {
        LOGGER.info(String.format("Event message received -> %s", eventMessage));
        try {
            // Optionally, validate the JSON format if needed
            objectMapper.readTree(eventMessage); // Throws JsonProcessingException if invalid JSON

            WikimediaData wikimediaData = new WikimediaData();
            wikimediaData.setWikiEventData(eventMessage);
            dataRepository.save(wikimediaData);

        } catch (JsonProcessingException e){
            // Handle JSON parsing errors
            LOGGER.error("Failed to process JSON event message: {}", eventMessage, e);
        } catch (Exception e) {
            // Handle any other exceptions, such as database errors
            LOGGER.error("An error occurred while saving data to the database: {}", eventMessage, e);
        }
    }
}
