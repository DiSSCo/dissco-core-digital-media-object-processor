package eu.dissco.core.digitalmediaobjectprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalmediaobjectprocessor.Profiles;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectEvent;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.DigitalSpecimenNotFoundException;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.NoChangesFoundException;
import javax.xml.transform.TransformerException;
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

  @KafkaListener(topics = "${kafka.consumer.topic}")
  public void getMessages(@Payload String message)
      throws JsonProcessingException, TransformerException, DigitalSpecimenNotFoundException {
    var event = mapper.readValue(message, DigitalMediaObjectEvent.class);
    try {
      processingService.handleMessage(event, false);
    } catch (NoChangesFoundException e) {
      log.info(e.getMessage());
    }
  }

}
