package eu.dissco.core.digitalmediaprocessor.service;

import static eu.dissco.core.digitalmediaprocessor.domain.AgentRoleType.PROCESSING_SERVICE;
import static eu.dissco.core.digitalmediaprocessor.domain.AgentRoleType.SOURCE_SYSTEM;
import static eu.dissco.core.digitalmediaprocessor.schema.Agent.Type.SCHEMA_SOFTWARE_APPLICATION;
import static eu.dissco.core.digitalmediaprocessor.schema.Identifier.DctermsType.DOI;
import static eu.dissco.core.digitalmediaprocessor.schema.Identifier.DctermsType.HANDLE;
import static eu.dissco.core.digitalmediaprocessor.utils.AgentUtils.createMachineAgent;
import static eu.dissco.core.digitalmediaprocessor.utils.DigitalMediaUtils.DOI_PREFIX;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalmediaprocessor.domain.AutoAcceptedAnnotation;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.domain.UpdatedDigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.properties.ApplicationProperties;
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

  public static final String NEW_INFORMATION_MESSAGE = "Received new information from Source System with id: ";
  public static final String OP = "op";
  public static final String FROM = "from";
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

    try {
      var annotationProcessingRequest = digitalMediaRecords.stream()
          .map(this::mapNewMediaToAnnotation)
          .toList();
      kafkaPublisherService.publishAcceptedAnnotation(
          new AutoAcceptedAnnotation(
              createMachineAgent(applicationProperties.getName(), applicationProperties.getPid(),
                  PROCESSING_SERVICE, DOI, SCHEMA_SOFTWARE_APPLICATION),
              annotationProcessingRequest));
    } catch (JsonProcessingException e) {
      log.error("Unable to send auto-accepted annotation for new media", e);
    }
  }

  private AnnotationProcessingRequest mapNewMediaToAnnotation(
      DigitalMediaRecord digitalMediaRecord) {
    var sourceSystemID = digitalMediaRecord.digitalMediaWrapper().attributes()
        .getOdsSourceSystemID();
    var sourceSystemName = digitalMediaRecord.digitalMediaWrapper().attributes()
        .getOdsSourceSystemName();
    try {
      return new AnnotationProcessingRequest()
          .withOaMotivation(OaMotivation.ODS_ADDING)
          .withOaMotivatedBy("New information received from Source System with id: "
              + sourceSystemID)
          .withOaHasBody(buildBody(mapper.writeValueAsString(
              DigitalMediaUtils.flattenToDigitalMedia(digitalMediaRecord)), sourceSystemID))
          .withOaHasTarget(buildTarget(digitalMediaRecord, buildNewMediaSelector()))
          .withDctermsCreated(Date.from(Instant.now()))
          .withDctermsCreator(
              createMachineAgent(sourceSystemName, sourceSystemID, SOURCE_SYSTEM, HANDLE,
                  SCHEMA_SOFTWARE_APPLICATION));
    } catch (JsonProcessingException e) {
      log.info("Unable to map new media to annotation: {}", digitalMediaRecord, e);
      return null;
    }
  }

  private AnnotationBody buildBody(String value, String sourceSystemID) {
    return new AnnotationBody()
        .withType("oa:TextualBody")
        .withOaValue(List.of(value))
        .withDctermsReferences(sourceSystemID);
  }

  private AnnotationTarget buildTarget(DigitalMediaRecord digitalMediaRecord,
      OaHasSelector selector) {
    var targetId = DOI_PREFIX + digitalMediaRecord.id();
    return new AnnotationTarget()
        .withId(targetId)
        .withDctermsIdentifier(targetId)
        .withType(digitalMediaRecord.digitalMediaWrapper().type())
        .withOdsFdoType("https://doi.org/21.T11148/bbad8c4e101e8af01115")
        .withOaHasSelector(selector);
  }

  public void publishAnnotationUpdatedMedia(
      Set<UpdatedDigitalMediaRecord> updatedDigitalMediaRecords) {

    try {
      var annotations = updatedDigitalMediaRecords.stream().map(updatedDigitalMediaRecord ->
          convertJsonPatchToAnnotations(
              updatedDigitalMediaRecord.digitalMediaRecord(),
              updatedDigitalMediaRecord.jsonPatch()))
          .flatMap(List::stream)
          .toList();
      if (!annotations.isEmpty()) {
          kafkaPublisherService.publishAcceptedAnnotation(new AutoAcceptedAnnotation(
              createMachineAgent(applicationProperties.getName(), applicationProperties.getPid(),
                  PROCESSING_SERVICE, DOI, SCHEMA_SOFTWARE_APPLICATION),
              annotations));
      }
    } catch (JsonProcessingException e) {
      log.error("Unable to send auto-accepted annotation for updated media.", e);
    }
  }

  private List<AnnotationProcessingRequest> convertJsonPatchToAnnotations(
      DigitalMediaRecord digitalMediaRecord, JsonNode jsonNode) {
    var annotations = new ArrayList<AnnotationProcessingRequest>();
    var sourceSystemID = digitalMediaRecord.digitalMediaWrapper().attributes()
        .getOdsSourceSystemID();
    var sourceSystemName = digitalMediaRecord.digitalMediaWrapper().attributes()
        .getOdsSourceSystemName();
    for (JsonNode action : jsonNode) {
      var annotationProcessingRequest = new AnnotationProcessingRequest()
          .withOaHasTarget(buildTarget(digitalMediaRecord,
              buildMediaSelector(action.get("path").asText())))
          .withDctermsCreated(Date.from(Instant.now()))
          .withDctermsCreator(
              createMachineAgent(sourceSystemName, sourceSystemID, SOURCE_SYSTEM, HANDLE,
                  SCHEMA_SOFTWARE_APPLICATION));
      try {
        if (action.get(OP).asText().equals("replace")) {
          annotations.add(addReplaceOperation(action, annotationProcessingRequest, sourceSystemID));
        } else if (action.get(OP).asText().equals("add")) {
          annotations.add(addAddOperation(action, annotationProcessingRequest, sourceSystemID));
        } else if (action.get(OP).asText().equals("remove")) {
          annotations.add(addRemoveOperation(annotationProcessingRequest, sourceSystemID));
        } else if (action.get(OP).asText().equals("copy")) {
          var annotation = addCopyOperation(digitalMediaRecord, action, annotationProcessingRequest,
              sourceSystemID);
          if (annotation != null) {
            annotations.add(annotation);
          }
        } else if (action.get(OP).asText().equals("move")) {
          annotations.addAll(
              addMoveOperation(digitalMediaRecord, action, annotationProcessingRequest,
                  sourceSystemID,
                  sourceSystemName));
        }
      } catch (JsonProcessingException e) {
        log.error("Unable to map jsonPatch to annotation: {}", action, e);
      }
    }
    return annotations;
  }

  private List<AnnotationProcessingRequest> addMoveOperation(
      DigitalMediaRecord digitalMediaRecord, JsonNode action,
      AnnotationProcessingRequest annotationProcessingRequest, String sourceSystemID,
      String sourceSystemName)
      throws JsonProcessingException {
    var digitalMediaJson = mapper.convertValue(
        digitalMediaRecord.digitalMediaWrapper().attributes(), JsonNode.class);
    var valueNode = digitalMediaJson.at(action.get(FROM).asText());
    annotationProcessingRequest.setOaMotivation(OaMotivation.ODS_ADDING);
    annotationProcessingRequest.setOaMotivatedBy(NEW_INFORMATION_MESSAGE + sourceSystemID);
    annotationProcessingRequest.setOaHasBody(
        buildBody(extractValueString(valueNode), sourceSystemID));
    var additionalDeleteAnnotation = new AnnotationProcessingRequest()
        .withOaHasTarget(buildTarget(digitalMediaRecord,
            buildMediaSelector(action.get(FROM).asText())))
        .withDctermsCreated(Date.from(Instant.now()))
        .withDctermsCreator(
            createMachineAgent(sourceSystemName, sourceSystemID, SOURCE_SYSTEM, HANDLE,
                SCHEMA_SOFTWARE_APPLICATION))
        .withOaMotivation(OaMotivation.ODS_DELETING)
        .withOaMotivatedBy(
            "Received delete information from Source System with id: " + sourceSystemID);
    return List.of(annotationProcessingRequest, additionalDeleteAnnotation);
  }

  private AnnotationProcessingRequest addCopyOperation(DigitalMediaRecord digitalMediaRecord,
      JsonNode action, AnnotationProcessingRequest annotationProcessingRequest,
      String sourceSystemID)
      throws JsonProcessingException {
    var digitalMediaJson = mapper.convertValue(
        digitalMediaRecord.digitalMediaWrapper().attributes(), JsonNode.class);
    var valueNode = digitalMediaJson.at(action.get(FROM).asText());
    if (!valueNode.isMissingNode()) {
      annotationProcessingRequest.setOaMotivation(OaMotivation.ODS_ADDING);
      annotationProcessingRequest.setOaMotivatedBy(NEW_INFORMATION_MESSAGE + sourceSystemID);
      annotationProcessingRequest.setOaHasBody(
          buildBody(extractValueString(valueNode), sourceSystemID));
      return annotationProcessingRequest;
    } else {
      log.warn("Invalid copy operation in json patch: {} Ignoring this annotation", action);
      return null;
    }
  }

  private AnnotationProcessingRequest addRemoveOperation(
      AnnotationProcessingRequest annotationProcessingRequest,
      String sourceSystemID) {
    annotationProcessingRequest.setOaMotivation(OaMotivation.ODS_DELETING);
    annotationProcessingRequest.setOaMotivatedBy(
        "Received delete information from Source System with id: " + sourceSystemID);
    return annotationProcessingRequest;
  }

  private AnnotationProcessingRequest addAddOperation(JsonNode action,
      AnnotationProcessingRequest annotationProcessingRequest, String sourceSystemID)
      throws JsonProcessingException {
    annotationProcessingRequest.setOaMotivation(OaMotivation.ODS_ADDING);
    annotationProcessingRequest.setOaMotivatedBy(NEW_INFORMATION_MESSAGE + sourceSystemID);
    annotationProcessingRequest.setOaHasBody(
        buildBody(extractValueString(action.get(VALUE)), sourceSystemID));
    return annotationProcessingRequest;
  }

  private AnnotationProcessingRequest addReplaceOperation(JsonNode action,
      AnnotationProcessingRequest annotationProcessingRequest, String sourceSystemID)
      throws JsonProcessingException {
    annotationProcessingRequest.setOaMotivation(OaMotivation.OA_EDITING);
    annotationProcessingRequest.setOaMotivatedBy(
        "Received update information from Source System with id: " + sourceSystemID);
    annotationProcessingRequest.setOaHasBody(
        buildBody(extractValueString(action.get(VALUE)), sourceSystemID));
    return annotationProcessingRequest;
  }

  private String extractValueString(JsonNode action) throws JsonProcessingException {
    if (action.isTextual()) {
      return action.textValue();
    } else {
      return mapper.writeValueAsString(action);
    }
  }

  private OaHasSelector buildMediaSelector(String action) {
    var path = convertJsonPointToJsonPath(action);
    if (action.endsWith("/-")) {
      return new OaHasSelector()
          .withAdditionalProperty(TYPE, "ods:ClassSelector")
          .withAdditionalProperty("ods:class", path);
    } else {
      return new OaHasSelector()
          .withAdditionalProperty(TYPE, "ods:TermSelector")
          .withAdditionalProperty("ods:term", path);
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
        jsonPath.append("['").append(part).append("']");
      }
    }
    return jsonPath.toString();
  }

  private boolean isNumeric(String str) {
    return numericPattern.matcher(str).matches();
  }
}
