package eu.dissco.core.digitalmediaobjectprocessor.service;

import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.DIGITAL_OBJECT_TYPE;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.FDO_PROFILE;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.ISSUED_FOR_AGENT;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.LICENSE_URL;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.MEDIA_FORMAT;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.PRIMARY_MEDIA_ID;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.PRIMARY_MO_TYPE;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.REFERENT_NAME;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.LINKED_DO_PID;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.TYPE;
import static eu.dissco.core.digitalmediaobjectprocessor.service.ServiceUtils.getMediaUrl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
    attributes.put(REFERENT_NAME.getAttribute(),mediaObject.type() + " for " + mediaObject.digitalSpecimenId());
    attributes.put(PRIMARY_MEDIA_ID.getAttribute(), getMediaUrl(mediaObject.attributes()));
    attributes.put(PRIMARY_MO_TYPE.getAttribute(), "resolvable");
    attributes.put(LINKED_DO_PID.getAttribute(), mediaObject.digitalSpecimenId());

    if (mediaObject.attributes().get("dcterms:license") != null){
      attributes.put(LICENSE_URL.getAttribute(), mediaObject.attributes().get("dcterms:license").asText());
    }
    if (attributes.get("dcterms:type") != null){
      attributes.put(MEDIA_FORMAT.getAttribute(), "image");
    }

    return attributes;
  }

  public JsonNode buildRollbackCreationRequest(List<DigitalMediaObjectRecord> digitalMediaObjects) {
    var handles = digitalMediaObjects.stream().map(DigitalMediaObjectRecord::id).toList();
    var dataNode = handles.stream().map(handle -> mapper.createObjectNode().put("id", handle))
        .toList();
    ArrayNode dataArray = mapper.valueToTree(dataNode);
    return mapper.createObjectNode().set("data", dataArray);
  }

  public List<JsonNode> buildPatchDeleteRequest(
      List<DigitalMediaObjectRecord> digitalSpecimenRecords) {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var media : digitalSpecimenRecords) {
      requestBody.add(buildSinglePatchDeleteRequest(media));
    }
    return requestBody;
  }

  private JsonNode buildSinglePatchDeleteRequest(DigitalMediaObjectRecord mediaObject) {
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
      DigitalMediaObject mediaObject) {
    return (!currentMediaObject.digitalSpecimenId().equals(mediaObject.digitalSpecimenId())
        || !Objects.equals(getMediaUrl(currentMediaObject.attributes()),
        getMediaUrl(mediaObject.attributes())));
  }

}
