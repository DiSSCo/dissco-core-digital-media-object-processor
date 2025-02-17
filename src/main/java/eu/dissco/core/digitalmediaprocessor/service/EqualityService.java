package eu.dissco.core.digitalmediaprocessor.service;

import static com.jayway.jsonpath.JsonPath.using;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaEvent;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaWrapper;
import eu.dissco.core.digitalmediaprocessor.schema.EntityRelationship;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class EqualityService {

  private final Configuration jsonPathConfiguration;
  private final ObjectMapper mapper;

  private static final List<String> IGNORED_FIELDS = List.of(
      "dcterms:created",
      "dcterms:modified",
      "dwc:relationshipEstablishedDate"
  );

  public boolean isEqual(DigitalMediaWrapper currentDigitalMedia,
      DigitalMediaWrapper digitalMedia) {
    if (currentDigitalMedia == null || currentDigitalMedia.attributes() == null) {
      return false;
    }
    try {
      verifyOriginalData(currentDigitalMedia, digitalMedia);
      var jsonCurrentDigitalMedia = normaliseJsonNode(mapper.valueToTree(currentDigitalMedia.attributes()));
      var jsonDigitalMedia = normaliseJsonNode(mapper.valueToTree(digitalMedia.attributes()));
      var isEqual = jsonCurrentDigitalMedia.equals(jsonDigitalMedia);
      if (!isEqual) {
        log.debug("Specimen {} has changed. JsonDiff: {}",
            currentDigitalMedia.attributes().getDctermsIdentifier(),
            JsonDiff.asJson(jsonCurrentDigitalMedia, jsonDigitalMedia));
      }
      return isEqual;
    } catch (JsonProcessingException e) {
      log.error("Unable to re-serialize JSON. Can not determine equality.", e);
      return false;
    }
  }

  private JsonNode normaliseJsonNode(JsonNode digitalMedia) throws JsonProcessingException {
    var context = using(jsonPathConfiguration).parse(mapper.writeValueAsString(digitalMedia));
    removeGeneratedTimestamps(context);
    return mapper.valueToTree(context.jsonString());
  }

  private static void removeGeneratedTimestamps(DocumentContext context) {
    IGNORED_FIELDS.forEach(field -> {
      // Find paths of target field
      var paths = new HashSet<String>(context.read("$..[?(@." + field + ")]"));
      // Set each value of the given path to null
      paths.forEach(path -> {
        var fullPath = path + "['" + field + "']";
        context.delete(fullPath);
      });
    });
  }

  public DigitalMediaEvent setEventDates(
      DigitalMediaRecord currentDigitalMediaWrapper,
      DigitalMediaEvent digitalMediaEvent) {
    var digitalMedia = digitalMediaEvent.digitalMediaWrapper().attributes();
    setEntityRelationshipDates(
        currentDigitalMediaWrapper.digitalMediaWrapper().attributes().getOdsHasEntityRelationships(),
        digitalMedia.getOdsHasEntityRelationships());
    // Set dcterms:created to original date
    digitalMedia.withDctermsCreated(
        currentDigitalMediaWrapper.digitalMediaWrapper().attributes().getDctermsCreated());
    // We create a new object because the events/wrappers are immutable, and we don't want the hash code to be out of sync
    return new DigitalMediaEvent(digitalMediaEvent.enrichmentList(),
        new DigitalMediaWrapper(
            digitalMediaEvent.digitalMediaWrapper().type(),
            digitalMediaEvent.digitalMediaWrapper().digitalSpecimenID(),
            digitalMedia,
            digitalMediaEvent.digitalMediaWrapper().originalAttributes()));
  }


  private void setEntityRelationshipDates(List<EntityRelationship> currentEntityRelationships,
      List<EntityRelationship> entityRelationships) {
    // Create a map with relatedResourceID as a key so we only compare potentially equal ERs
    // This reduces complexity compared to nested for-loops
    var currentEntityRelationshipsMap = currentEntityRelationships.stream()
        .collect(
            Collectors.toMap(EntityRelationship::getDwcRelatedResourceID, Function.identity()));
    entityRelationships.forEach(entityRelationship -> {
      var currentEntityRelationship = currentEntityRelationshipsMap.get(
          entityRelationship.getDwcRelatedResourceID());
      if (entityRelationshipsAreEqual(currentEntityRelationship, entityRelationship)) {
        entityRelationship.setDwcRelationshipEstablishedDate(
            currentEntityRelationship.getDwcRelationshipEstablishedDate());
      }
    });
  }

  private boolean entityRelationshipsAreEqual(EntityRelationship currentEntityRelationship,
      EntityRelationship entityRelationship) {
    try {
      var jsonCurrentEntityRelationship = normaliseJsonNode(
          mapper.valueToTree(currentEntityRelationship));
      var jsonEntityRelationship = normaliseJsonNode(mapper.valueToTree(entityRelationship));
      return jsonCurrentEntityRelationship.equals(jsonEntityRelationship);
    } catch (JsonProcessingException e) {
      log.error("Unable to serialize entity relationships", e);
      return false;
    }
  }

  private static void verifyOriginalData(DigitalMediaWrapper currentDigitalMediaWrapper,
      DigitalMediaWrapper digitalMediaWrapper) {
    if (currentDigitalMediaWrapper.originalAttributes() != null
        && !currentDigitalMediaWrapper.originalAttributes()
        .equals(digitalMediaWrapper.originalAttributes())) {
      log.info(
          "Media Object with ac:accessURI {} has changed original data. New original data not captured.",
          digitalMediaWrapper.attributes().getAcAccessURI());
    }
  }

}
