package eu.dissco.core.digitalmediaprocessor.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.diff.JsonDiff;
import eu.dissco.core.digitalmediaprocessor.component.SourceSystemNameComponent;
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
  private final SourceSystemNameComponent sourceSystemNameComponent;

  public CreateUpdateTombstoneEvent generateCreateEvent(
      DigitalMediaRecord digitalMediaRecord) {
    var digitalSpecimen = DigitalMediaUtils.flattenToDigitalMedia(digitalMediaRecord);
    return generateCreateUpdateTombStoneEvent(digitalSpecimen, ProvActivity.Type.ODS_CREATE,
        null);
  }

  private CreateUpdateTombstoneEvent generateCreateUpdateTombStoneEvent(
      DigitalMedia digitalMedia, ProvActivity.Type activityType,
      JsonNode jsonPatch) {
    var entityID = digitalMedia.getOdsID() + "/" + digitalMedia.getOdsVersion();
    var activityID = UUID.randomUUID().toString();
    var sourceSystemID = digitalMedia.getOdsSourceSystemID();
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
            .withRdfsComment("Specimen newly created"))
        .withProvEntity(new ProvEntity()
            .withId(entityID)
            .withType(digitalMedia.getType())
            .withProvValue(mapEntityToProvValue(digitalMedia))
            .withProvWasGeneratedBy(activityID))
        .withOdsHasProvAgent(List.of(
            new Agent()
                .withType(Type.AS_APPLICATION)
                .withId(sourceSystemID)
                .withSchemaName(sourceSystemNameComponent.getSourceSystemName(sourceSystemID)),
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
      DigitalMediaRecord currentDigitalMediaRecord) {
    var digitalSpecimen = DigitalMediaUtils.flattenToDigitalMedia(digitalMediaRecord);
    var currentDigitalSpecimen = DigitalMediaUtils.flattenToDigitalMedia(
        currentDigitalMediaRecord);
    var jsonPatch = createJsonPatch(currentDigitalSpecimen, digitalSpecimen);
    return generateCreateUpdateTombStoneEvent(digitalSpecimen, ProvActivity.Type.ODS_UPDATE,
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

  private JsonNode createJsonPatch(DigitalMedia currentDigitalMedia,
      DigitalMedia digitalSpecimenMedia) {
    return JsonDiff.asJson(mapper.valueToTree(currentDigitalMedia),
        mapper.valueToTree(digitalSpecimenMedia));
  }
}
