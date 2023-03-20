package eu.dissco.core.digitalmediaobjectprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObject;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectEvent;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectRecord;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectTransfer;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalSpecimenInformation;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.DigitalSpecimenNotFoundException;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.NoChangesFoundException;
import eu.dissco.core.digitalmediaobjectprocessor.repository.DigitalMediaObjectRepository;
import eu.dissco.core.digitalmediaobjectprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalmediaobjectprocessor.repository.ElasticSearchRepository;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import javax.xml.transform.TransformerException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessingService {

  private static final int SUCCESS = 1;

  private final DigitalMediaObjectRepository repository;
  private final DigitalSpecimenRepository digitalSpecimenRepository;
  private final HandleService handleService;
  private final ElasticSearchRepository elasticRepository;
  private final KafkaPublisherService kafkaService;

  private static DigitalMediaObject convertToDigitalMediaObject(
      DigitalMediaObjectTransfer digitalMediaObjectTransfer,
      DigitalSpecimenInformation digitalSpecimenInformation) {
    var attributes = digitalMediaObjectTransfer.attributes();
    return new DigitalMediaObject(digitalMediaObjectTransfer.type(),
        digitalSpecimenInformation.id(),
        attributes.get("ac:accessURI").asText(),
        attributes.get("dcterms:format").asText(),
        attributes.get("ods:sourceSystemId").asText(),
        attributes,
        digitalMediaObjectTransfer.originalAttributes());
  }

  public DigitalMediaObjectRecord handleMessage(DigitalMediaObjectEvent event, boolean webProfile)
      throws JsonProcessingException, TransformerException, DigitalSpecimenNotFoundException, NoChangesFoundException {
    var digitalMediaObjectTransfer = event.digitalMediaObject();
    var digitalSpecimenInformationOptional = retrieveDigitalSpecimenId(digitalMediaObjectTransfer);
    if (digitalSpecimenInformationOptional.isEmpty()) {
      digitalSpecimenMissing(event, webProfile, digitalMediaObjectTransfer);
      return null;
    } else {
      var digitalSpecimenInformation = digitalSpecimenInformationOptional.get();
      var digitalMediaObject = convertToDigitalMediaObject(digitalMediaObjectTransfer,
          digitalSpecimenInformation);
      var currentDigitalMediaObjectOptional = repository.getDigitalMediaObject(
          digitalSpecimenInformation.id(),
          digitalMediaObject.mediaUrl());
      if (currentDigitalMediaObjectOptional.isEmpty()) {
        return persistNewDigitalMediaObject(digitalMediaObject, event.enrichmentList());
      } else {
        var currentDigitalMediaObject = currentDigitalMediaObjectOptional.get();
        if (currentDigitalMediaObject.digitalMediaObject().equals(digitalMediaObject)) {
          log.info("Received digitalMediaObject is equal to digitalMediaObject: {}",
              currentDigitalMediaObject.id());
          processEqualDigitalSpecimen(currentDigitalMediaObject);
          throw new NoChangesFoundException("No changes were necessary to mediaObject with id: "
              + currentDigitalMediaObject.id());
        } else {
          log.info("DigitalMediaObject with id: {} has received an update",
              currentDigitalMediaObject.id());
          return updateExistingDigitalSpecimen(currentDigitalMediaObject, digitalMediaObject);
        }
      }
    }
  }

  private void digitalSpecimenMissing(DigitalMediaObjectEvent event, boolean webProfile,
      DigitalMediaObjectTransfer digitalMediaObjectTransfer)
      throws DigitalSpecimenNotFoundException, JsonProcessingException {
    log.error("Digital specimen for dwca_id: {} and sourceSystem: {} is not available",
        digitalMediaObjectTransfer.physicalSpecimenId(),
        digitalMediaObjectTransfer.attributes().get("ods:sourceSystemId").asText());
    if (webProfile) {
      throw new DigitalSpecimenNotFoundException(
          "Digital Specimen with id: " + digitalMediaObjectTransfer.physicalSpecimenId()
              + " was not found");
    } else {
      kafkaService.republishDigitalMediaObject(event);
    }
  }

  private DigitalMediaObjectRecord updateExistingDigitalSpecimen(
      DigitalMediaObjectRecord currentDigitalMediaObject,
      DigitalMediaObject digitalMediaObject) {
    if (handleNeedsUpdate(currentDigitalMediaObject.digitalMediaObject(), digitalMediaObject)) {
      handleService.updateHandle(currentDigitalMediaObject.id(), digitalMediaObject);
    }
    var id = currentDigitalMediaObject.id();
    var version = currentDigitalMediaObject.version() + 1;
    var digitalMediaObjectRecord = new DigitalMediaObjectRecord(id, version, Instant.now(),
        digitalMediaObject);
    var result = repository.createDigitalMediaObjectRecord(digitalMediaObjectRecord);
    if (result == SUCCESS) {
      log.info("DigitalMediaObject: {} has been successfully committed to database", id);
      var indexDocument = elasticRepository.indexDigitalMediaObject(digitalMediaObjectRecord);
      if (indexDocument.result().jsonValue().equals("updated")) {
        log.info("DigitalMediaObject: {} has been successfully indexed", id);
        kafkaService.publishUpdateEvent(currentDigitalMediaObject, digitalMediaObjectRecord);
      }
    }
    return digitalMediaObjectRecord;
  }

  private boolean handleNeedsUpdate(DigitalMediaObject currentDigitalMediaObject,
      DigitalMediaObject digitalMediaObject) {
    return !currentDigitalMediaObject.type().equals(digitalMediaObject.type());
  }

  private void processEqualDigitalSpecimen(DigitalMediaObjectRecord currentDigitalMediaObject) {
    var result = repository.updateLastChecked(currentDigitalMediaObject);
    if (result == SUCCESS) {
      log.info("Successfully updated lastChecked for existing digitalMediaObject: {}",
          currentDigitalMediaObject.id());
    }
  }

  private DigitalMediaObjectRecord persistNewDigitalMediaObject(
      DigitalMediaObject digitalMediaObject, List<String> enrichmentList)
      throws TransformerException {
    var id = handleService.createNewHandle(digitalMediaObject);
    log.info("New id has been generated for MultiMediaObject: {}", id);
    var digitalMediaObjectRecord = new DigitalMediaObjectRecord(id, 1, Instant.now(),
        digitalMediaObject);
    var result = repository.createDigitalMediaObjectRecord(digitalMediaObjectRecord);
    if (result == SUCCESS) {
      log.info("DigitalMediaObject: {} has been successfully committed to database", id);
      var indexDocument = elasticRepository.indexDigitalMediaObject(digitalMediaObjectRecord);
      if (indexDocument.result().jsonValue().equals("created")) {
        log.info("Specimen: {} has been successfully indexed", id);
        kafkaService.publishCreateEvent(digitalMediaObjectRecord);
        for (var enrichment : enrichmentList) {
          kafkaService.publishAnnotationRequestEvent(enrichment, digitalMediaObjectRecord);
        }
      }
    }
    return digitalMediaObjectRecord;
  }

  private Optional<DigitalSpecimenInformation> retrieveDigitalSpecimenId(
      DigitalMediaObjectTransfer digitalMediaObject) {
      return digitalSpecimenRepository.getSpecimenId(digitalMediaObject.physicalSpecimenId());
  }
}
