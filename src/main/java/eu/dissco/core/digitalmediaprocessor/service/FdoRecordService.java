package eu.dissco.core.digitalmediaprocessor.service;

import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.ISSUED_FOR_AGENT;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.IS_DERIVED_FROM_SPECIMEN;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.LICENSE_NAME;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.LINKED_DO_PID;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.LINKED_DO_TYPE;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.MEDIA_FORMAT;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.MEDIA_HOST;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.PRIMARY_MEDIA_ID;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.PRIMARY_MO_ID_NAME;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.PRIMARY_MO_ID_TYPE;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.DCTERMS_TYPE;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.REFERENT_NAME;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.RIGHTSHOLDER_PID_TYPE;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.TYPE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaWrapper;
import eu.dissco.core.digitalmediaprocessor.exceptions.PidCreationException;
import eu.dissco.core.digitalmediaprocessor.properties.FdoProperties;
import eu.dissco.core.digitalmediaprocessor.schema.DigitalMedia;
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
  private static final String MISSING_ELEMENT_MSG = "Missing mandatory fdo element %s";

  public List<JsonNode> buildPostHandleRequest(List<DigitalMediaWrapper> mediaObjects)
      throws PidCreationException {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var mediaObject : mediaObjects) {
      requestBody.add(buildSingleHandleRequest(mediaObject));
    }
    return requestBody;
  }

  private JsonNode buildSingleHandleRequest(DigitalMediaWrapper mediaObject)
      throws PidCreationException {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    var attributes = generateAttributes(mediaObject);
    data.put(TYPE.getAttribute(), fdoProperties.getType());
    data.set("attributes", attributes);
    request.set("data", data);
    return request;
  }

  private JsonNode generateAttributes(DigitalMediaWrapper mediaObject) throws PidCreationException {
    validateMandatoryAttributes(mediaObject.attributes());
    var attributes = mapper.createObjectNode();
    attributes.put(MEDIA_HOST.getAttribute(),
        mediaObject.attributes().getOdsOrganisationID());
    attributes.put(LICENSE_NAME.getAttribute(), mediaObject.attributes().getDctermsLicense());
    attributes.put(PRIMARY_MEDIA_ID.getAttribute(), mediaObject.attributes().getAcAccessURI());
    attributes.put(REFERENT_NAME.getAttribute(),
        mediaObject.type() + " for " + mediaObject.digitalSpecimenID());
    attributes.put(LINKED_DO_PID.getAttribute(), mediaObject.digitalSpecimenID());
    if (mediaObject.type() != null) {
      attributes.put(MEDIA_FORMAT.getAttribute(), MEDIA_FORMAT.getDefaultValue());
    }
    // Default
    attributes.put(MEDIA_FORMAT.getAttribute(), MEDIA_FORMAT.getDefaultValue());
    attributes.put(DCTERMS_TYPE.getAttribute(), DCTERMS_TYPE.getDefaultValue());
    attributes.put(ISSUED_FOR_AGENT.getAttribute(), fdoProperties.getIssuedForAgent());
    attributes.put(PRIMARY_MO_ID_TYPE.getAttribute(), PRIMARY_MO_ID_TYPE.getDefaultValue());
    attributes.put(PRIMARY_MO_ID_NAME.getAttribute(), PRIMARY_MO_ID_NAME.getDefaultValue());
    attributes.put(RIGHTSHOLDER_PID_TYPE.getAttribute(), RIGHTSHOLDER_PID_TYPE.getDefaultValue());
    attributes.put(IS_DERIVED_FROM_SPECIMEN.getAttribute(),
        Boolean.valueOf(IS_DERIVED_FROM_SPECIMEN.getDefaultValue()));
    attributes.put(LINKED_DO_TYPE.getAttribute(), LINKED_DO_TYPE.getDefaultValue());
    return attributes;
  }

  public List<String> buildRollbackCreationRequest(List<DigitalMediaRecord> digitalMedia) {
    return digitalMedia.stream().map(DigitalMediaRecord::id).toList();
  }

  private void validateMandatoryAttributes(DigitalMedia mediaObject) throws PidCreationException {
    if (mediaObject.getOdsOrganisationID() == null) {
      log.error(String.format(MISSING_ELEMENT_MSG, "ods:organisationID"));
      throw new PidCreationException(String.format(MISSING_ELEMENT_MSG, "ods:organisationID"));
    }
    if (mediaObject.getDctermsLicense() == null) {
      log.error(String.format(MISSING_ELEMENT_MSG, "dcterms:license"));
      throw new PidCreationException(String.format(MISSING_ELEMENT_MSG, "dcterms:license"));
    }
  }

  public List<JsonNode> buildPatchDeleteRequest(
      List<DigitalMediaRecord> digitalSpecimenRecords) throws PidCreationException {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var media : digitalSpecimenRecords) {
      requestBody.add(buildSinglePatchDeleteRequest(media));
    }
    return requestBody;
  }

  private JsonNode buildSinglePatchDeleteRequest(DigitalMediaRecord mediaObject)
      throws PidCreationException {
    var request = mapper.createObjectNode();
    var data = mapper.createObjectNode();
    var attributes = generateAttributes(mediaObject.digitalMediaWrapper());
    data.put(TYPE.getAttribute(), fdoProperties.getType());
    data.put("id", mediaObject.id());
    data.set("attributes", attributes);
    request.set("data", data);
    return request;
  }

  public boolean handleNeedsUpdate(DigitalMediaWrapper currentMediaObject,
      DigitalMediaWrapper mediaObject) {
    return (!currentMediaObject.digitalSpecimenID().equals(mediaObject.digitalSpecimenID())
        || !currentMediaObject.attributes().getAcAccessURI()
        .equals(mediaObject.attributes().getAcAccessURI())
        || (currentMediaObject.attributes().getDctermsLicense() != null
        && !currentMediaObject.attributes().getDctermsLicense()
        .equals(mediaObject.attributes().getDctermsLicense()))
        || (currentMediaObject.type() != null && !currentMediaObject.type()
        .equals(mediaObject.type()))
        || (currentMediaObject.attributes().getDctermsType() != null
        && !currentMediaObject.attributes().getDctermsType()
        .equals(mediaObject.attributes().getDctermsType()))
        || (currentMediaObject.attributes().getOdsOrganisationID() != null
        && !currentMediaObject.attributes().getOdsOrganisationID()
        .equals(mediaObject.attributes().getOdsOrganisationID())));
  }

}
