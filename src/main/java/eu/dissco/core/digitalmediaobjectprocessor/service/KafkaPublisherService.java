package eu.dissco.core.digitalmediaobjectprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.diff.JsonDiff;
import eu.dissco.core.digitalmediaobjectprocessor.domain.CreateUpdateDeleteEvent;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectEvent;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectRecord;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaPublisherService {

  private final ObjectMapper mapper;
  private final KafkaTemplate<String, String> kafkaTemplate;

  public void publishCreateEvent(DigitalMediaObjectRecord digitalMediaObjectRecord) {
    var event = new CreateUpdateDeleteEvent(UUID.randomUUID(), "create",
        "digital-media-object-processing-service", digitalMediaObjectRecord.id(), Instant.now(),
        mapper.valueToTree(digitalMediaObjectRecord), "Digital Media Object newly created");
    try {
      kafkaTemplate.send("createUpdateDeleteTopic", mapper.writeValueAsString(event));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public void publishAnnotationRequestEvent(String enrichment,
      DigitalMediaObjectRecord digitalMediaObjectRecord) {
    try {
      kafkaTemplate.send(enrichment, mapper.writeValueAsString(digitalMediaObjectRecord));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public void publishUpdateEvent(DigitalMediaObjectRecord currentDigitalMediaRecord,
      DigitalMediaObjectRecord digitalMediaObjectRecord) {
    var jsonPatch = createJsonPatch(currentDigitalMediaRecord, digitalMediaObjectRecord);
    var event = new CreateUpdateDeleteEvent(UUID.randomUUID(), "update", "digital-media-object-processing-service",
        digitalMediaObjectRecord.id(), Instant.now(), jsonPatch,
        "Digital Media Object has been updated");
    try {
      kafkaTemplate.send("createUpdateDeleteTopic", mapper.writeValueAsString(event));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private JsonNode createJsonPatch(DigitalMediaObjectRecord currentDigitalMediaRecord,
      DigitalMediaObjectRecord digitalMediaObjectRecord) {
    return JsonDiff.asJson(mapper.valueToTree(currentDigitalMediaRecord.digitalMediaObject()),
        mapper.valueToTree(digitalMediaObjectRecord.digitalMediaObject()));
  }

  public void republishDigitalMediaObject(DigitalMediaObjectEvent event) throws JsonProcessingException {
    kafkaTemplate.send("digital-media-object", mapper.writeValueAsString(event));
  }
}
