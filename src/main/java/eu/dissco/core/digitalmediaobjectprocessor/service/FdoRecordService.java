package eu.dissco.core.digitalmediaobjectprocessor.service;

import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.DIGITAL_OBJECT_TYPE;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.FDO_PROFILE;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.ISSUED_FOR_AGENT;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.REFERENT_NAME;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.TYPE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObject;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class FdoRecordService {

  private final ObjectMapper mapper;

  public List<JsonNode> buildPostHandleRequest(List<DigitalMediaObject> mediaObjects) {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var mediaObject : mediaObjects) {
      requestBody.add(buildSingleHandleRequest(mediaObject));
    }
    return requestBody;
  }

  private JsonNode buildSingleHandleRequest(DigitalMediaObject mediaObject) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    var attributes = generateAttributes(mediaObject);
    data.put(TYPE.getAttribute(), TYPE.getDefaultValue());
    data.set("attributes", attributes);
    request.set("data", data);
    return request;
  }

  private JsonNode generateAttributes(DigitalMediaObject mediaObject) {
    var attributes = mapper.createObjectNode();
    attributes.put(FDO_PROFILE.getAttribute(), FDO_PROFILE.getDefaultValue());
    attributes.put(DIGITAL_OBJECT_TYPE.getAttribute(), DIGITAL_OBJECT_TYPE.getDefaultValue());
    attributes.put(ISSUED_FOR_AGENT.getAttribute(), ISSUED_FOR_AGENT.getDefaultValue());
    attributes.put(REFERENT_NAME.getAttribute(),
        mediaObject.type() + " for " + mediaObject.physicalSpecimenId());
    return attributes;
  }

  public JsonNode buildRollbackCreationRequest(List<DigitalMediaObjectRecord> digitalMediaObjects) {
    var handles = digitalMediaObjects.stream().map(DigitalMediaObjectRecord::id).toList();
    var dataNode = handles.stream()
        .map(handle -> mapper.createObjectNode().put("id", handle))
        .toList();
    ArrayNode dataArray = mapper.valueToTree(dataNode);
    return mapper.createObjectNode().set("data", dataArray);
  }

  public List<JsonNode> buildRollbackUpdateRequest(
      List<DigitalMediaObjectRecord> digitalSpecimenRecords) {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var media : digitalSpecimenRecords) {
      requestBody.add(buildSingleRollbackUpdateRequest(media));
    }
    return requestBody;
  }

  private JsonNode buildSingleRollbackUpdateRequest(DigitalMediaObjectRecord mediaObject) {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    var attributes = generateAttributes(mediaObject.digitalMediaObject());
    data.put(TYPE.getAttribute(), TYPE.getDefaultValue());
    data.put("id", mediaObject.id());
    data.set("attributes", attributes);
    request.set("data", data);
    return request;
  }

  public boolean handleNeedsUpdate(DigitalMediaObject currentMediaObject,
      DigitalMediaObject digitalSpecimen) {
    return false;
  }

  private boolean isUnequal(DigitalMediaObject currentMediaObject,
      DigitalMediaObject newMediaObject,
      String fieldName) {
    return !Objects.equals(currentMediaObject.attributes().findValue(fieldName).asText(),
        newMediaObject.attributes().findValue(fieldName).asText());
  }

}
