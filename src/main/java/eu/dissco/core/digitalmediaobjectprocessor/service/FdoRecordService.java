package eu.dissco.core.digitalmediaobjectprocessor.service;

import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.ISSUED_FOR_AGENT;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.IS_DERIVED_FROM_SPECIMEN;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.LICENSE;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.LINKED_DO_PID;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.LINKED_DO_TYPE;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.MEDIA_FORMAT;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.MEDIA_HOST;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.PRIMARY_MEDIA_ID;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.PRIMARY_MO_ID_NAME;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.PRIMARY_MO_ID_TYPE;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.PRIMARY_MO_TYPE;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.REFERENT_NAME;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.RIGHTSHOLDER_PID_TYPE;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.TYPE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectWrapper;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectRecord;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.PidCreationException;
import eu.dissco.core.digitalmediaobjectprocessor.properties.FdoProperties;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class FdoRecordService {

  private final ObjectMapper mapper;
  private final FdoProperties fdoProperties;

  public List<JsonNode> buildPostHandleRequest(List<DigitalMediaObjectWrapper> mediaObjects)
      throws PidCreationException {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var mediaObject : mediaObjects) {
      requestBody.add(buildSingleHandleRequest(mediaObject));
    }
    return requestBody;
  }

  private JsonNode buildSingleHandleRequest(DigitalMediaObjectWrapper mediaObject)
      throws PidCreationException {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    var attributes = generateAttributes(mediaObject);
    data.put(TYPE.getAttribute(), fdoProperties.getType());
    data.set("attributes", attributes);
    request.set("data", data);
    return request;
  }

  private JsonNode generateAttributes(DigitalMediaObjectWrapper mediaObject) throws PidCreationException {
    var attributes = mapper.createObjectNode();
    if (mediaObject.attributes().getDwcInstitutionId() != null
        && mediaObject.attributes().getDctermsLicense() != null
        && mediaObject.attributes().getAcAccessUri() != null) {
      attributes.put(MEDIA_HOST.getAttribute(),
          mediaObject.attributes().getDwcInstitutionId());
      attributes.put(LICENSE.getAttribute(),
          mediaObject.attributes().getDctermsLicense());
      attributes.put(PRIMARY_MEDIA_ID.getAttribute(), mediaObject.attributes().getAcAccessUri());
    } else {
      log.error("Missing mandatory element for FDO profile");
      throw new PidCreationException("Missing mandatory element for FDO profile");
    }
    attributes.put(REFERENT_NAME.getAttribute(),
        mediaObject.type() + " for " + mediaObject.digitalSpecimenId());
    attributes.put(LINKED_DO_PID.getAttribute(), mediaObject.digitalSpecimenId());
    if (mediaObject.type() != null) {
      attributes.put(MEDIA_FORMAT.getAttribute(), MEDIA_FORMAT.getDefaultValue());
    }

    // Default values
    attributes.put(ISSUED_FOR_AGENT.getAttribute(), fdoProperties.getIssuedForAgent());
    attributes.put(PRIMARY_MO_TYPE.getAttribute(), PRIMARY_MO_TYPE.getDefaultValue());
    attributes.put(PRIMARY_MO_ID_TYPE.getAttribute(), PRIMARY_MO_ID_TYPE.getDefaultValue());
    attributes.put(PRIMARY_MO_ID_NAME.getAttribute(), PRIMARY_MO_ID_NAME.getDefaultValue());
    attributes.put(RIGHTSHOLDER_PID_TYPE.getAttribute(), RIGHTSHOLDER_PID_TYPE.getDefaultValue());
    attributes.put(IS_DERIVED_FROM_SPECIMEN.getAttribute(),
        Boolean.valueOf(IS_DERIVED_FROM_SPECIMEN.getDefaultValue()));
    attributes.put(LINKED_DO_TYPE.getAttribute(), LINKED_DO_TYPE.getDefaultValue());

    return attributes;
  }

  public JsonNode buildRollbackCreationRequest
      (List<DigitalMediaObjectRecord> digitalMediaObjects) {
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
    var attributes = generateAttributes(mediaObject.digitalMediaObjectWrapper());
    data.put(TYPE.getAttribute(), fdoProperties.getType());
    data.put("id", mediaObject.id());
    data.set("attributes", attributes);
    request.set("data", data);
    return request;
  }

  public boolean handleNeedsUpdate(DigitalMediaObjectWrapper currentMediaObject,
      DigitalMediaObjectWrapper mediaObject) {
    return (!currentMediaObject.digitalSpecimenId().equals(mediaObject.digitalSpecimenId())
        || !currentMediaObject.attributes().getAcAccessUri()
        .equals(mediaObject.attributes().getAcAccessUri())
        || (currentMediaObject.attributes().getDctermsLicense() != null
        && !currentMediaObject.attributes().getDctermsLicense()
        .equals(mediaObject.attributes().getDctermsLicense()))
        || (currentMediaObject.type() != null && !currentMediaObject.type()
        .equals(mediaObject.type()))
        || (currentMediaObject.attributes().getDctermsType() != null
        && !currentMediaObject.attributes().getDctermsType()
        .equals(mediaObject.attributes().getDctermsType()))
        || (currentMediaObject.attributes().getDwcInstitutionId() != null
        && !currentMediaObject.attributes().getDwcInstitutionId()
        .equals(mediaObject.attributes().getDwcInstitutionId())));
  }

}
