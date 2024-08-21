package eu.dissco.core.digitalmediaprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaEvent;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaUpdatePidEvent;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaPublisherService {

  private final ObjectMapper mapper;
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final ProvenanceService provenanceService;

  public void publishCreateEvent(DigitalMediaRecord digitalMediaRecord)
      throws JsonProcessingException {
    var event = provenanceService.generateCreateEvent(digitalMediaRecord);
    kafkaTemplate.send("createUpdateDeleteTopic", mapper.writeValueAsString(event));
  }

  public void publishAnnotationRequestEvent(String enrichment,
      DigitalMediaRecord digitalMediaRecord) throws JsonProcessingException {
    kafkaTemplate.send(enrichment, mapper.writeValueAsString(digitalMediaRecord));
  }

  public void publishUpdateEvent(DigitalMediaRecord digitalMediaRecord,
      DigitalMediaRecord currentDigitalMediaRecord) throws JsonProcessingException {
    var event = provenanceService.generateUpdateEvent(digitalMediaRecord,
        currentDigitalMediaRecord);
    kafkaTemplate.send("createUpdateDeleteTopic", mapper.writeValueAsString(event));
  }

  public void republishDigitalMedia(DigitalMediaEvent event)
      throws JsonProcessingException {
    kafkaTemplate.send("digital-media-object", mapper.writeValueAsString(event));
  }

  public void deadLetterRaw(String message) {
    kafkaTemplate.send("digital-media-object-dlq", message);
  }

  public void deadLetterEvent(DigitalMediaEvent event)
      throws JsonProcessingException {
    kafkaTemplate.send("digital-media-object-dlq", mapper.writeValueAsString(event));
  }

  public void publishMediaPid(List<DigitalMediaUpdatePidEvent> event) throws JsonProcessingException {
    kafkaTemplate.send("media-update", mapper.writeValueAsString(event));
  }

}
