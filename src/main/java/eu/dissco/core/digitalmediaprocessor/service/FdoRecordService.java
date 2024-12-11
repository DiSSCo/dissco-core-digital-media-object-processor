package eu.dissco.core.digitalmediaprocessor.service;

import static eu.dissco.core.digitalmediaprocessor.domain.AgentRoleType.RIGHTS_OWNER;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.LICENSE_URL;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.LICENSE_NAME;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.LINKED_DO_PID;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.LINKED_DO_TYPE;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.MEDIA_HOST;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.MEDIA_HOST_NAME;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.MEDIA_ID;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.MEDIA_ID_NAME;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.MEDIA_ID_TYPE;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.MEDIA_TYPE;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.MIME_TYPE;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.REFERENT_NAME;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.RIGHTS_HOLDER_PID;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.RIGHTS_HOLDER;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.RIGHTS_HOLDER_PID;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.RIGHTS_HOLDER;
import static eu.dissco.core.digitalmediaprocessor.utils.DigitalMediaUtils.DOI_PREFIX;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaWrapper;
import eu.dissco.core.digitalmediaprocessor.properties.FdoProperties;
import eu.dissco.core.digitalmediaprocessor.schema.Agent;
import eu.dissco.core.digitalmediaprocessor.schema.DigitalMedia;
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
  private final FdoProperties fdoProperties;
  private static final String URL_PATTERN = "http(s)?://.+";

  public List<JsonNode> buildPostHandleRequest(List<DigitalMediaWrapper> mediaObjects) {
    return mediaObjects.stream().map(this::buildSingleHandleRequest).toList();
  }

  private JsonNode buildSingleHandleRequest(DigitalMediaWrapper mediaObject) {
    return mapper.createObjectNode()
        .set("data", mapper.createObjectNode()
            .put("type", fdoProperties.getMediaType())
            .set("attributes", generateAttributes(mediaObject)));
  }

  private JsonNode generateAttributes(DigitalMediaWrapper mediaObject) {
    var media = mediaObject.attributes();
    var attributes = mapper.createObjectNode()
        .put(REFERENT_NAME.getAttribute(), media.getAcAccessURI())
        .put(MEDIA_HOST.getAttribute(), media.getOdsOrganisationID())
        .put(MEDIA_HOST_NAME.getAttribute(), media.getOdsOrganisationName())
        .put(LINKED_DO_PID.getAttribute(), mediaObject.digitalSpecimenID())
        .put(LINKED_DO_TYPE.getAttribute(), fdoProperties.getSpecimenType())
        .put(MEDIA_ID.getAttribute(), media.getAcAccessURI())
        .put(MEDIA_ID_TYPE.getAttribute(), "Resolvable")
        .put(MEDIA_ID_NAME.getAttribute(), "ac:accessURI")
        .put(MEDIA_TYPE.getAttribute(), "Image")
        .put(MIME_TYPE.getAttribute(), media.getDctermsFormat());
    setLicense(attributes, media);
    setRightsHolder(attributes, media);
    return attributes;
  }

  private static void setLicense(ObjectNode attributes, DigitalMedia media) {
    if (media.getDctermsRights() != null && media.getDctermsRights().matches(URL_PATTERN)) {
      attributes.put(LICENSE_URL.getAttribute(), media.getDctermsRights());
    } else if (media.getDctermsRights() != null) {
      attributes.put(LICENSE_NAME.getAttribute(), media.getDctermsRights());
    }
  }

  private static void setRightsHolder(ObjectNode attributes, DigitalMedia media) {
    var rightsHolderId = collectRightsHolder(media, false);
    var rightsHolderName = collectRightsHolder(media, true);
    if (rightsHolderId != null) {
      attributes.put(RIGHTS_HOLDER_PID.getAttribute(), rightsHolderId);
    }
    if (rightsHolderName != null) {
      attributes.put(RIGHTS_HOLDER.getAttribute(), rightsHolderName);
    }
  }

  private static String collectRightsHolder(DigitalMedia media, boolean name) {
    var rightsHolderStream = media.getOdsHasAgents().stream()
        .filter(agent -> agent.getOdsHasRoles().stream()
            .anyMatch(role -> role.getSchemaRoleName().equals(RIGHTS_OWNER.getName())));
    if (name) {
      return rightsHolderStream.map(Agent::getSchemaName).filter(Objects::nonNull)
          .reduce((a, b) -> a + " | " + b)
          .orElse(null);
    } else {
      return rightsHolderStream.map(Agent::getId).filter(Objects::nonNull)
          .reduce((a, b) -> a + " | " + b).orElse(null);
    }
  }

  public List<String> buildRollbackCreationRequest(List<DigitalMediaRecord> digitalMedia) {
    return digitalMedia.stream().map(DigitalMediaRecord::id).toList();
  }

  public List<JsonNode> buildPatchDeleteRequest(List<DigitalMediaRecord> digitalSpecimenRecords) {
    List<JsonNode> requestBody = new ArrayList<>();
    for (var media : digitalSpecimenRecords) {
      requestBody.add(buildSinglePatchDeleteRequest(media));
    }
    return requestBody;
  }

  private JsonNode buildSinglePatchDeleteRequest(DigitalMediaRecord mediaRecord) {
    return mapper.createObjectNode()
        .set("data", mapper.createObjectNode()
            .put("type", fdoProperties.getMediaType())
            .put("id", mediaRecord.id().replace(DOI_PREFIX, ""))
            .set("attributes", generateAttributes(mediaRecord.digitalMediaWrapper())));
  }

  public boolean handleNeedsUpdate(DigitalMediaWrapper currentMediaObject,
      DigitalMediaWrapper mediaObject) {
    return (!currentMediaObject.digitalSpecimenID().equals(mediaObject.digitalSpecimenID())
        || !currentMediaObject.attributes().getAcAccessURI()
        .equals(mediaObject.attributes().getAcAccessURI())
        || (currentMediaObject.attributes().getDctermsRights() != null
        && !currentMediaObject.attributes().getDctermsRights()
        .equals(mediaObject.attributes().getDctermsRights()))
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
