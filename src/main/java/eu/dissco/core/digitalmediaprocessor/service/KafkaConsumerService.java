package eu.dissco.core.digitalmediaprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalmediaprocessor.Profiles;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaEvent;
import eu.dissco.core.digitalmediaprocessor.exceptions.DigitalSpecimenNotFoundException;
import java.util.List;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@Profile(Profiles.KAFKA)
@AllArgsConstructor
public class KafkaConsumerService {

  private final ObjectMapper mapper;
  private final ProcessingService processingService;
  private final KafkaPublisherService publisherService;

  @KafkaListener(topics = "${kafka.consumer.topic}")
  public void getMessages(@Payload List<String> messages) {
    var events = messages.stream().map(message -> {
      try {
        return mapper.readValue(message, DigitalMediaEvent.class);
      } catch (JsonProcessingException e) {
        log.error("Moving message to DLQ, failed to parse event message", e);
        publisherService.deadLetterRaw(message);
        return null;
      }
    }).filter(Objects::nonNull).toList();
    if (!events.isEmpty()) {
      try {
        processingService.handleMessage(events);
      } catch (DigitalSpecimenNotFoundException e) {
        log.error("Throw an exception which should only be thrown in the web profile", e);
      }
    } else {
      log.info("No more message to process in batch");
    }
  }

}
