package eu.dissco.core.digitalmediaprocessor.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.properties.ApplicationProperties;
import eu.dissco.core.digitalmediaprocessor.schema.Agent;
import eu.dissco.core.digitalmediaprocessor.schema.Agent.Type;
import eu.dissco.core.digitalmediaprocessor.schema.CreateUpdateTombstoneEvent;
import eu.dissco.core.digitalmediaprocessor.schema.DigitalMedia;
import eu.dissco.core.digitalmediaprocessor.schema.OdsChangeValue;
import eu.dissco.core.digitalmediaprocessor.schema.ProvActivity;
import eu.dissco.core.digitalmediaprocessor.schema.ProvEntity;
import eu.dissco.core.digitalmediaprocessor.schema.ProvValue;
import eu.dissco.core.digitalmediaprocessor.schema.ProvWasAssociatedWith;
import eu.dissco.core.digitalmediaprocessor.schema.ProvWasAssociatedWith.ProvHadRole;
import eu.dissco.core.digitalmediaprocessor.utils.DigitalMediaUtils;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ProvenanceService {

  private final ObjectMapper mapper;
  private final ApplicationProperties properties;

  public CreateUpdateTombstoneEvent generateCreateEvent(
      DigitalMediaRecord digitalMediaRecord) {
    var digitalMedia = DigitalMediaUtils.flattenToDigitalMedia(digitalMediaRecord);
    return generateCreateUpdateTombStoneEvent(digitalMedia, ProvActivity.Type.ODS_CREATE,
        null);
  }

  private CreateUpdateTombstoneEvent generateCreateUpdateTombStoneEvent(
      DigitalMedia digitalMedia, ProvActivity.Type activityType,
      JsonNode jsonPatch) {
    var entityID = digitalMedia.getOdsID() + "/" + digitalMedia.getOdsVersion();
    var activityID = UUID.randomUUID().toString();
    var sourceSystemID = digitalMedia.getOdsSourceSystemID();
    var sourceSystemName = digitalMedia.getOdsSourceSystemName();
    return new CreateUpdateTombstoneEvent()
        .withId(entityID)
        .withType("ods:CreateUpdateTombstoneEvent")
        .withOdsID(entityID)
        .withOdsType(properties.getCreateUpdateTombstoneEventType())
        .withProvActivity(new ProvActivity()
            .withId(activityID)
            .withType(activityType)
            .withOdsChangeValue(mapJsonPatch(jsonPatch))
            .withProvEndedAtTime(Date.from(Instant.now()))
            .withProvWasAssociatedWith(List.of(
                new ProvWasAssociatedWith()
                    .withId(sourceSystemID)
                    .withProvHadRole(ProvHadRole.ODS_REQUESTOR),
                new ProvWasAssociatedWith()
                    .withId(properties.getPid())
                    .withProvHadRole(ProvHadRole.ODS_APPROVER),
                new ProvWasAssociatedWith()
                    .withId(properties.getPid())
                    .withProvHadRole(ProvHadRole.ODS_GENERATOR)))
            .withProvUsed(entityID)
            .withRdfsComment("Digital Media newly created"))
        .withProvEntity(new ProvEntity()
            .withId(entityID)
            .withType("ods:DigitalMedia")
            .withProvValue(mapEntityToProvValue(digitalMedia))
            .withProvWasGeneratedBy(activityID))
        .withOdsHasProvAgent(List.of(
            new Agent()
                .withType(Type.AS_APPLICATION)
                .withId(sourceSystemID)
                .withSchemaName(sourceSystemName),
            new Agent()
                .withType(Type.AS_APPLICATION)
                .withId(properties.getPid())
                .withSchemaName(properties.getName())
        ));
  }

  private List<OdsChangeValue> mapJsonPatch(JsonNode jsonPatch) {
    if (jsonPatch == null) {
      return null;
    }
    return mapper.convertValue(jsonPatch, new TypeReference<>() {
    });
  }

  public CreateUpdateTombstoneEvent generateUpdateEvent(DigitalMediaRecord digitalMediaRecord,
      JsonNode jsonPatch) {
    var digitalMedia = DigitalMediaUtils.flattenToDigitalMedia(digitalMediaRecord);
    return generateCreateUpdateTombStoneEvent(digitalMedia, ProvActivity.Type.ODS_UPDATE,
        jsonPatch);
  }

  private ProvValue mapEntityToProvValue(DigitalMedia digitalMedia) {
    var provValue = new ProvValue();
    var node = mapper.convertValue(digitalMedia, new TypeReference<Map<String, Object>>() {
    });
    for (var entry : node.entrySet()) {
      provValue.setAdditionalProperty(entry.getKey(), entry.getValue());
    }
    return provValue;
  }
}
