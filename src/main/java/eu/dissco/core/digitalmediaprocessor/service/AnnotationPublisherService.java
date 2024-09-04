package eu.dissco.core.digitalmediaprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalmediaprocessor.domain.AutoAcceptedAnnotation;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.domain.UpdatedDigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.properties.ApplicationProperties;
import eu.dissco.core.digitalmediaprocessor.schema.Agent;
import eu.dissco.core.digitalmediaprocessor.schema.Agent.Type;
import eu.dissco.core.digitalmediaprocessor.schema.AnnotationBody;
import eu.dissco.core.digitalmediaprocessor.schema.AnnotationProcessingRequest;
import eu.dissco.core.digitalmediaprocessor.schema.AnnotationProcessingRequest.OaMotivation;
import eu.dissco.core.digitalmediaprocessor.schema.AnnotationTarget;
import eu.dissco.core.digitalmediaprocessor.schema.OaHasSelector;
import eu.dissco.core.digitalmediaprocessor.utils.DigitalMediaUtils;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class AnnotationPublisherService {

  private static final String VALUE = "value";
  private static final String TYPE = "@type";
  private final Pattern numericPattern = Pattern.compile("\\d+");

  private final KafkaPublisherService kafkaPublisherService;
  private final ApplicationProperties applicationProperties;
  private final ObjectMapper mapper;

  private static OaHasSelector buildNewMediaSelector() {
    return new OaHasSelector()
        .withAdditionalProperty(TYPE, "ods:ClassSelector")
        .withAdditionalProperty("ods:class", "$");
  }

  public void publishAnnotationNewMedia(Set<DigitalMediaRecord> digitalMediaRecords) {
    for (var digitalMediaRecord : digitalMediaRecords) {
      try {
        var annotationProcessingRequest = mapNewMediaToAnnotation(digitalMediaRecord);
        kafkaPublisherService.publishAcceptedAnnotation(
            new AutoAcceptedAnnotation(
                new Agent().withId(applicationProperties.getPid())
                    .withSchemaName(applicationProperties.getName())
                    .withType(Type.AS_APPLICATION),
                annotationProcessingRequest));
      } catch (JsonProcessingException e) {
        log.error("Unable to send auto-accepted annotation for new media: {}",
            digitalMediaRecord.id(), e);
      }
    }
  }

  private AnnotationProcessingRequest mapNewMediaToAnnotation(
      DigitalMediaRecord digitalMediaRecord) throws JsonProcessingException {
    var sourceSystemID = digitalMediaRecord.digitalMediaWrapper().attributes()
        .getOdsSourceSystemID();
    var sourceSystemName = digitalMediaRecord.digitalMediaWrapper().attributes()
        .getOdsSourceSystemName();
    return new AnnotationProcessingRequest()
        .withOaMotivation(OaMotivation.ODS_ADDING)
        .withOaMotivatedBy("New information received from Source System with id: "
            + sourceSystemID)
        .withOaHasBody(buildBody(mapper.writeValueAsString(
            DigitalMediaUtils.flattenToDigitalMedia(digitalMediaRecord)), sourceSystemID))
        .withOaHasTarget(buildTarget(digitalMediaRecord, buildNewMediaSelector()))
        .withDctermsCreated(Date.from(Instant.now()))
        .withDctermsCreator(
            new Agent().withType(Type.AS_APPLICATION).withId(sourceSystemID)
                .withSchemaName(sourceSystemName));
  }

  private AnnotationBody buildBody(String value, String sourceSystemID) {
    return new AnnotationBody()
        .withType("oa:TextualBody")
        .withOaValue(List.of(value))
        .withDctermsReferences(sourceSystemID);
  }

  private AnnotationTarget buildTarget(DigitalMediaRecord digitalMediaRecord,
      OaHasSelector selector) {
    return new AnnotationTarget()
        .withId(digitalMediaRecord.id())
        .withOdsID(digitalMediaRecord.id())
        .withType(digitalMediaRecord.digitalMediaWrapper().type())
        .withOdsType("ods:DigitalMedia")
        .withOaHasSelector(selector);
  }

  public void publishAnnotationUpdatedMedia(
      Set<UpdatedDigitalMediaRecord> updatedDigitalMediaRecords) {
    for (var updatedDigitalMediaRecord : updatedDigitalMediaRecords) {
      try {
        var annotations = convertJsonPatchToAnnotations(
            updatedDigitalMediaRecord.digitalMediaRecord(),
            updatedDigitalMediaRecord.jsonPatch());
        for (var annotationProcessingRequest : annotations) {
          kafkaPublisherService.publishAcceptedAnnotation(new AutoAcceptedAnnotation(
              new Agent().withId(applicationProperties.getPid())
                  .withSchemaName(applicationProperties.getName())
                  .withType(Type.AS_APPLICATION),
              annotationProcessingRequest));
        }
      } catch (JsonProcessingException e) {
        log.error("Unable to send auto-accepted annotation for updated media: {}",
            updatedDigitalMediaRecord.digitalMediaRecord().id(), e);
      }
    }
  }


  private List<AnnotationProcessingRequest> convertJsonPatchToAnnotations(
      DigitalMediaRecord digitalMediaRecord, JsonNode jsonPatch)
      throws JsonProcessingException {
    var annotations = new ArrayList<AnnotationProcessingRequest>();
    var sourceSystemID = digitalMediaRecord.digitalMediaWrapper().attributes()
        .getOdsSourceSystemID();
    var sourceSystemName = digitalMediaRecord.digitalMediaWrapper().attributes()
        .getOdsSourceSystemName();
    for (var action : jsonPatch) {
      var annotationProcessingRequest = new AnnotationProcessingRequest()
          .withOaHasTarget(buildTarget(digitalMediaRecord, buildMediaSelector(action)))
          .withDctermsCreated(Date.from(Instant.now()))
          .withDctermsCreator(
              new Agent().withType(Type.AS_APPLICATION).withId(sourceSystemID)
                  .withSchemaName(sourceSystemName));
      if (action.get("op").asText().equals("replace")) {
        annotationProcessingRequest.setOaMotivation(OaMotivation.OA_EDITING);
        annotationProcessingRequest.setOaMotivatedBy(
            "Received update information from Source System with id: " + sourceSystemID);
        annotationProcessingRequest.setOaHasBody(
            buildBody(extractValueString(action), sourceSystemID));
      } else if (action.get("op").asText().equals("add")) {
        annotationProcessingRequest.setOaMotivation(OaMotivation.ODS_ADDING);
        annotationProcessingRequest.setOaMotivatedBy(
            "Received new information from Source System with id: " + sourceSystemID);
        annotationProcessingRequest.setOaHasBody(
            buildBody(extractValueString(action), sourceSystemID));
      } else if (action.get("op").asText().equals("remove")) {
        annotationProcessingRequest.setOaMotivation(OaMotivation.ODS_DELETING);
        annotationProcessingRequest.setOaMotivatedBy(
            "Received delete information from Source System with id: " + sourceSystemID);
      }
      annotations.add(annotationProcessingRequest);
    }
    return annotations;
  }

  private String extractValueString(JsonNode action) throws JsonProcessingException {
    if (action.get(VALUE).isTextual()) {
      return action.get(VALUE).textValue();
    } else {
      return mapper.writeValueAsString(action.get(VALUE));
    }
  }

  private OaHasSelector buildMediaSelector(JsonNode action) {
    var pointer = action.get("path").asText();
    var path = convertJsonPointToJsonPath(pointer);
    if (action.get("path").asText().endsWith("/-")) {
      return new OaHasSelector()
          .withAdditionalProperty(TYPE, "ods:ClassSelector")
          .withAdditionalProperty("ods:class", path);
    } else {
      return new OaHasSelector()
          .withAdditionalProperty(TYPE, "ods:FieldSelector")
          .withAdditionalProperty("ods:field", path);
    }
  }

  public String convertJsonPointToJsonPath(String jsonPointer) {
    String[] parts = jsonPointer.split("/");
    StringBuilder jsonPath = new StringBuilder("$");

    // Start from 1 to ignore the first root element
    for (int i = 1; i < parts.length; i++) {
      String part = parts[i];
      if (isNumeric(part)) {
        jsonPath.append("[").append(part).append("]");
      } else if (!part.equals("-")) {
        jsonPath.append(".").append(part);
      }
    }
    return jsonPath.toString();
  }

  private boolean isNumeric(String str) {
    return numericPattern.matcher(str).matches();
  }
}
