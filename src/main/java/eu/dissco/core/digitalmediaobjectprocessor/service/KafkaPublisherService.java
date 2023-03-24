package eu.dissco.core.digitalmediaobjectprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.diff.JsonDiff;
import eu.dissco.core.digitalmediaobjectprocessor.domain.CreateUpdateDeleteEvent;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectRecord;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectTransferEvent;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaPublisherService {

  private static final String SUBJECT_TYPE = "DigitalMediaObject";

  private final ObjectMapper mapper;
  private final KafkaTemplate<String, String> kafkaTemplate;

  public void publishCreateEvent(DigitalMediaObjectRecord digitalMediaObjectRecord)
      throws JsonProcessingException {
    var event = new CreateUpdateDeleteEvent(UUID.randomUUID(), "create",
        "digital-media-object-processing-service",
        digitalMediaObjectRecord.id(),
        SUBJECT_TYPE,
        Instant.now(),
        mapper.valueToTree(digitalMediaObjectRecord),
        null,
        "Digital Media Object newly created");
    kafkaTemplate.send("createUpdateDeleteTopic", mapper.writeValueAsString(event));
  }

  public void publishAnnotationRequestEvent(String enrichment,
      DigitalMediaObjectRecord digitalMediaObjectRecord) throws JsonProcessingException {
    kafkaTemplate.send(enrichment, mapper.writeValueAsString(digitalMediaObjectRecord));
  }

  public void publishUpdateEvent(DigitalMediaObjectRecord digitalMediaObjectRecord,
      DigitalMediaObjectRecord currentDigitalMediaRecord) throws JsonProcessingException {
    var jsonPatch = createJsonPatch(currentDigitalMediaRecord, digitalMediaObjectRecord);
    var event = new CreateUpdateDeleteEvent(UUID.randomUUID(),
        "update",
        "digital-media-object-processing-service",
        digitalMediaObjectRecord.id(),
        SUBJECT_TYPE,
        Instant.now(),
        mapper.valueToTree(digitalMediaObjectRecord),
        jsonPatch,
        "Digital Media Object has been updated");
    kafkaTemplate.send("createUpdateDeleteTopic", mapper.writeValueAsString(event));
  }

  private JsonNode createJsonPatch(DigitalMediaObjectRecord currentDigitalMediaRecord,
      DigitalMediaObjectRecord digitalMediaObjectRecord) {
    return JsonDiff.asJson(mapper.valueToTree(currentDigitalMediaRecord.digitalMediaObject()),
        mapper.valueToTree(digitalMediaObjectRecord.digitalMediaObject()));
  }

  public void republishDigitalMediaObject(DigitalMediaObjectTransferEvent event)
      throws JsonProcessingException {
    kafkaTemplate.send("digital-media-object", mapper.writeValueAsString(event));
  }

  public void deadLetterRaw(String message) {
    kafkaTemplate.send("digital-media-object-dlq", message);
  }

  public void deadLetterEvent(DigitalMediaObjectTransferEvent event)
      throws JsonProcessingException {
    kafkaTemplate.send("digital-media-object-dlq", mapper.writeValueAsString(event));
  }
}
