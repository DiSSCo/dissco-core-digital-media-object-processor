package eu.dissco.core.digitalmediaprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalmediaprocessor.domain.AutoAcceptedAnnotation;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaEvent;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaRecord;
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

  public void publishUpdateEvent(DigitalMediaRecord digitalMediaRecord, JsonNode jsonPatch)
      throws JsonProcessingException {
    var event = provenanceService.generateUpdateEvent(digitalMediaRecord, jsonPatch);
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

  public void publishAcceptedAnnotation(AutoAcceptedAnnotation annotation)
      throws JsonProcessingException {
    kafkaTemplate.send("auto-accepted-annotation", mapper.writeValueAsString(annotation));
  }
}
