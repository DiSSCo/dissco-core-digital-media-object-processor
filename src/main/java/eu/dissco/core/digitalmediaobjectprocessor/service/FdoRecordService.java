package eu.dissco.core.digitalmediaobjectprocessor.service;

import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.DIGITAL_OBJECT_TYPE;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.FDO_PROFILE;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.ISSUED_FOR_AGENT;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.IS_DERIVED_FROM_SPECIMEN;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.LICENSE;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.LINKED_DO_TYPE;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.MEDIA_FORMAT;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.MEDIA_HOST;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.PRIMARY_MEDIA_ID;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.PRIMARY_MO_ID_NAME;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.PRIMARY_MO_ID_TYPE;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.PRIMARY_MO_TYPE;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.REFERENT_NAME;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.LINKED_DO_PID;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.RIGHTSHOLDER_PID_TYPE;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.TYPE;
import static eu.dissco.core.digitalmediaobjectprocessor.service.ServiceUtils.getMediaUrl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObject;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectRecord;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.PidCreationException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class FdoRecordService {

  private final ObjectMapper mapper;

  private static final String LICENSE_FIELD = "dcterms:license";
  private static final String TYPE_FIELD = "dcterms:type";

  public List<JsonNode> buildPostHandleRequest(List<DigitalMediaObject> mediaObjects)
      throws PidCreationException {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var mediaObject : mediaObjects) {
      requestBody.add(buildSingleHandleRequest(mediaObject));
    }
    return requestBody;
  }

  private JsonNode buildSingleHandleRequest(DigitalMediaObject mediaObject)
      throws PidCreationException {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    var attributes = generateAttributes(mediaObject);
    data.put(TYPE.getAttribute(), TYPE.getDefaultValue());
    data.set("attributes", attributes);
    request.set("data", data);
    return request;
  }

  private JsonNode generateAttributes(DigitalMediaObject mediaObject) throws PidCreationException {
    var attributes = mapper.createObjectNode();
    try {
      attributes.put(MEDIA_HOST.getAttribute(),
          mediaObject.attributes().get("ods:organisationId").asText());
      attributes.put(LICENSE.getAttribute(),
          mediaObject.attributes().get(LICENSE_FIELD).asText());
      attributes.put(PRIMARY_MEDIA_ID.getAttribute(), getMediaUrl(mediaObject.attributes()));
    } catch (NullPointerException npe) {
      log.error("Missing mandatory element for FDO profile", npe);
      throw new PidCreationException("Missing mandatory element for FDO profile");
    }
    attributes.put(REFERENT_NAME.getAttribute(),
        mediaObject.type() + " for " + mediaObject.digitalSpecimenId());
    attributes.put(LINKED_DO_PID.getAttribute(), mediaObject.digitalSpecimenId());
    if (mediaObject.attributes().get(TYPE_FIELD) != null) {
      attributes.put(MEDIA_FORMAT.getAttribute(), MEDIA_FORMAT.getDefaultValue());
    }

    // Default values
    attributes.put(FDO_PROFILE.getAttribute(), FDO_PROFILE.getDefaultValue());
    attributes.put(DIGITAL_OBJECT_TYPE.getAttribute(), DIGITAL_OBJECT_TYPE.getDefaultValue());
    attributes.put(ISSUED_FOR_AGENT.getAttribute(), ISSUED_FOR_AGENT.getDefaultValue());
    attributes.put(PRIMARY_MO_TYPE.getAttribute(), PRIMARY_MO_TYPE.getDefaultValue());
    attributes.put(PRIMARY_MO_ID_TYPE.getAttribute(), PRIMARY_MO_ID_TYPE.getDefaultValue());
    attributes.put(PRIMARY_MO_ID_NAME.getAttribute(), PRIMARY_MO_ID_NAME.getDefaultValue());
    attributes.put(RIGHTSHOLDER_PID_TYPE.getAttribute(), RIGHTSHOLDER_PID_TYPE.getDefaultValue());
    attributes.put(IS_DERIVED_FROM_SPECIMEN.getAttribute(),
        Boolean.valueOf(IS_DERIVED_FROM_SPECIMEN.getDefaultValue()));
    attributes.put(LINKED_DO_TYPE.getAttribute(), LINKED_DO_TYPE.getDefaultValue());

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
      List<DigitalMediaObjectRecord> digitalSpecimenRecords) throws PidCreationException {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var media : digitalSpecimenRecords) {
      requestBody.add(buildSinglePatchDeleteRequest(media));
    }
    return requestBody;
  }

  private JsonNode buildSinglePatchDeleteRequest(DigitalMediaObjectRecord mediaObject)
      throws PidCreationException {
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
        getMediaUrl(mediaObject.attributes()))
        || !currentMediaObject.attributes().get(LICENSE_FIELD)
        .equals(mediaObject.attributes().get(LICENSE_FIELD))
        || (currentMediaObject.attributes().get(TYPE_FIELD)!= null && !currentMediaObject.attributes().get(TYPE_FIELD)
        .equals(mediaObject.attributes().get(TYPE_FIELD)))
        || !currentMediaObject.type().equals(mediaObject.type()));
  }

}
