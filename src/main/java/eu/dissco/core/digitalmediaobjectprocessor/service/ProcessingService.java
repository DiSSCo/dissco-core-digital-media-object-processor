package eu.dissco.core.digitalmediaobjectprocessor.service;

import static java.util.stream.Collectors.toMap;

import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObject;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectEvent;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectKey;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectRecord;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectTransfer;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectTransferEvent;
import eu.dissco.core.digitalmediaobjectprocessor.domain.ProcessResult;
import eu.dissco.core.digitalmediaobjectprocessor.domain.UpdatedDigitalMediaRecord;
import eu.dissco.core.digitalmediaobjectprocessor.domain.UpdatedDigitalMediaTuple;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.DigitalSpecimenNotFoundException;
import eu.dissco.core.digitalmediaobjectprocessor.repository.DigitalMediaObjectRepository;
import eu.dissco.core.digitalmediaobjectprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalmediaobjectprocessor.repository.ElasticSearchRepository;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.xml.transform.TransformerException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessingService {

  private final DigitalMediaObjectRepository repository;
  private final DigitalSpecimenRepository digitalSpecimenRepository;
  private final HandleService handleService;
  private final ElasticSearchRepository elasticRepository;
  private final KafkaPublisherService kafkaService;

  private static String getMediaUrl(JsonNode attributes) {
    if (attributes.get("ac:accessURI") != null){
      return attributes.get("ac:accessURI").asText();
    }
    return null;
  }

  public List<DigitalMediaObjectRecord> handleMessage(List<DigitalMediaObjectTransferEvent> events,
      boolean webProfile) throws DigitalSpecimenNotFoundException {
    log.info("Processing {} digital specimen", events.size());
    var uniqueBatch = removeDuplicatesInBatch(events);
    var processResult = processDigitalMedia(uniqueBatch, webProfile);
    var results = new ArrayList<DigitalMediaObjectRecord>();
    if (!processResult.equalMediaObjects().isEmpty()) {
      processEqualDigitalMedia(processResult.equalMediaObjects());
    }
    if (!processResult.newMediaObjects().isEmpty()) {
      results.addAll(persistNewDigitalMediaObject(processResult.newMediaObjects()));
    }
    if (!processResult.changedMediaObjects().isEmpty()) {
      results.addAll(updateExistingDigitalSpecimen(processResult.changedMediaObjects()));
    }
    return results;
  }

  private Set<DigitalMediaObjectTransferEvent> removeDuplicatesInBatch(
      List<DigitalMediaObjectTransferEvent> events) {
    var uniqueSet = new HashSet<DigitalMediaObjectTransferEvent>();
    var map = events.stream()
        .collect(
            Collectors.groupingBy(event -> getMediaUrl(event.digitalMediaObject().attributes())));
    for (Entry<String, List<DigitalMediaObjectTransferEvent>> entry : map.entrySet()) {
      if (entry.getValue().size() > 1) {
        log.warn("Found {} duplicates in batch for id {}", entry.getValue().size(), entry.getKey());
        for (int i = 0; i < entry.getValue().size(); i++) {
          if (i == 0) {
            uniqueSet.add(entry.getValue().get(i));
          } else {
            republishEvent(entry.getValue().get(i));
          }
        }
      } else {
        uniqueSet.add(entry.getValue().get(0));
      }
    }
    return uniqueSet;
  }

  private void republishEvent(DigitalMediaObjectTransferEvent event) {
    try {
      kafkaService.republishDigitalMediaObject(event);
    } catch (JsonProcessingException e) {
      log.error("Fatal exception, unable to republish message due to invalid json", e);
    }
  }

  private ProcessResult processDigitalMedia(Set<DigitalMediaObjectTransferEvent> events,
      boolean webProfile)
      throws DigitalSpecimenNotFoundException {
    var convertedMediaObjects = getDigitalSpecimenId(events, webProfile);
    var currentMediaObjects = getCurrentDigitalMedia(
        convertedMediaObjects.stream().map(DigitalMediaObjectEvent::digitalMediaObject).toList());
    var equalMediaObjects = new ArrayList<DigitalMediaObjectRecord>();
    var changedMediaObjects = new ArrayList<UpdatedDigitalMediaTuple>();
    var newMediaObjects = new ArrayList<DigitalMediaObjectEvent>();

    for (var mediaObject : convertedMediaObjects) {
      var digitalMediaObject = mediaObject.digitalMediaObject();
      var digitalMediaObjectKey = new DigitalMediaObjectKey(
          digitalMediaObject.digitalSpecimenId(), getMediaUrl(digitalMediaObject.attributes()));
      log.debug("Processing digitalMediaObject: {}", digitalMediaObject);
      if (!currentMediaObjects.containsKey(digitalMediaObjectKey)) {
        log.debug("DigitalMedia with key: {} is completely new", digitalMediaObjectKey);
        newMediaObjects.add(mediaObject);
      } else {
        var currentDigitalSpecimen = currentMediaObjects.get(digitalMediaObjectKey);
        if (currentDigitalSpecimen.digitalMediaObject().equals(digitalMediaObject)) {
          log.debug("Received digital media is equal to digital media: {}",
              currentDigitalSpecimen.id());
          equalMediaObjects.add(currentDigitalSpecimen);
        } else {
          log.debug("Specimen with id: {} has received an update", currentDigitalSpecimen.id());
          changedMediaObjects.add(
              new UpdatedDigitalMediaTuple(currentDigitalSpecimen, mediaObject));
        }
      }
    }
    return new ProcessResult(equalMediaObjects, changedMediaObjects, newMediaObjects);
  }

  private List<DigitalMediaObjectEvent> getDigitalSpecimenId(
      Set<DigitalMediaObjectTransferEvent> events, boolean webProfile)
      throws DigitalSpecimenNotFoundException {
    var physicalSpecimenIds = events.stream()
        .map(event -> event.digitalMediaObject().physicalSpecimenId()).toList();
    var digitalSpecimenInformation = retrieveDigitalSpecimenId(physicalSpecimenIds);
    return convertToDigitalMediaObject(events, digitalSpecimenInformation, webProfile);
  }

  private List<DigitalMediaObjectEvent> convertToDigitalMediaObject(
      Set<DigitalMediaObjectTransferEvent> digitalMediaObjectEvents,
      Map<String, String> digitalSpecimenInformation, boolean webProfile)
      throws DigitalSpecimenNotFoundException {
    var convertedRecords = new ArrayList<DigitalMediaObjectEvent>();
    for (var mediaObjectEvent : digitalMediaObjectEvents) {
      if (digitalSpecimenInformation.containsKey(
          mediaObjectEvent.digitalMediaObject().physicalSpecimenId())) {
        convertedRecords.add(new DigitalMediaObjectEvent(
            mediaObjectEvent.enrichmentList(),
            new DigitalMediaObject(
                mediaObjectEvent.digitalMediaObject().type(),
                digitalSpecimenInformation.get(
                    mediaObjectEvent.digitalMediaObject().physicalSpecimenId()),
                mediaObjectEvent.digitalMediaObject().physicalSpecimenId(),
                mediaObjectEvent.digitalMediaObject().attributes(),
                mediaObjectEvent.digitalMediaObject().originalAttributes()))
        );
      } else {
        digitalSpecimenMissing(mediaObjectEvent, webProfile);
      }
    }
    return convertedRecords;
  }

  private Map<DigitalMediaObjectKey, DigitalMediaObjectRecord> getCurrentDigitalMedia(
      List<DigitalMediaObject> digitalMediaObjects) {
    return repository.getDigitalMediaObject(
            digitalMediaObjects.stream().map(DigitalMediaObject::digitalSpecimenId).toList(),
            digitalMediaObjects.stream().map(digitalMedia -> getMediaUrl(digitalMedia.attributes()))
                .toList())
        .stream().collect(
            toMap(digitalMediaRecord ->
                    new DigitalMediaObjectKey(
                        digitalMediaRecord.digitalMediaObject().digitalSpecimenId(),
                        getMediaUrl(digitalMediaRecord.digitalMediaObject().attributes())
                    ),
                Function.identity()));
  }

  private void digitalSpecimenMissing(DigitalMediaObjectTransferEvent event, boolean webProfile)
      throws DigitalSpecimenNotFoundException {
    log.error("Digital specimen for physicalSpecimen: {} and sourceSystem: {} is not available",
        event.digitalMediaObject().physicalSpecimenId(),
        event.digitalMediaObject().attributes().get("ods:sourceSystemId").asText());
    if (webProfile) {
      throw new DigitalSpecimenNotFoundException(
          "Digital Specimen with id: " + event.digitalMediaObject().physicalSpecimenId()
              + " was not found");
    } else {
      try {
        kafkaService.republishDigitalMediaObject(event);
      } catch (JsonProcessingException e) {
        log.error("Fatal exception, unable to republish message due to invalid json", e);
      }
    }
  }

  private Set<DigitalMediaObjectRecord> updateExistingDigitalSpecimen(
      List<UpdatedDigitalMediaTuple> updatedDigitalSpecimenTuples) {
    updateHandles(updatedDigitalSpecimenTuples);

    var digitalMediaRecords = getDigitalMediaRecordMap(updatedDigitalSpecimenTuples);
    log.info("Persisting to db");
    repository.createDigitalMediaRecord(
        digitalMediaRecords.stream().map(UpdatedDigitalMediaRecord::digitalMediaObjectRecord)
            .toList());
    log.info("Persisting to elastic");
    try {
      var bulkResponse = elasticRepository.indexDigitalMediaObject(
          digitalMediaRecords.stream().map(UpdatedDigitalMediaRecord::digitalMediaObjectRecord)
              .toList());
      if (!bulkResponse.errors()) {
        handleSuccessfulElasticUpdate(digitalMediaRecords);
      } else {
        handlePartiallyElasticUpdate(digitalMediaRecords, bulkResponse);
      }
      var successfullyProcessedRecords = digitalMediaRecords.stream()
          .map(UpdatedDigitalMediaRecord::digitalMediaObjectRecord).collect(
              Collectors.toSet());
      log.info("Successfully updated {} digitalSpecimen", successfullyProcessedRecords.size());
      return successfullyProcessedRecords;
    } catch (IOException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      digitalMediaRecords.forEach(
          updatedDigitalSpecimenRecord -> rollbackUpdatedDigitalMedia(updatedDigitalSpecimenRecord,
              false));
      return Set.of();
    }
  }

  private void handlePartiallyElasticUpdate(Set<UpdatedDigitalMediaRecord> digitalMediaRecords,
      BulkResponse bulkResponse) {
    var digitalSpecimenMap = digitalMediaRecords.stream()
        .collect(Collectors.toMap(
            updatedDigitalMediaRecord -> updatedDigitalMediaRecord.digitalMediaObjectRecord()
                .id(), Function.identity()));
    bulkResponse.items().forEach(
        item -> {
          var digitalMediaRecord = digitalSpecimenMap.get(item.id());
          if (item.error() != null) {
            log.error("Failed item to insert into elastic search: {} with errors {}",
                digitalMediaRecord.digitalMediaObjectRecord().id(), item.error().reason());
            rollbackUpdatedDigitalMedia(digitalMediaRecord, false);
            digitalMediaRecords.remove(digitalMediaRecord);
          } else {
            var successfullyPublished = publishUpdateEvent(digitalMediaRecord);
            if (!successfullyPublished) {
              digitalMediaRecords.remove(digitalMediaRecord);
            }
          }
        }
    );
  }

  private void handleSuccessfulElasticUpdate(Set<UpdatedDigitalMediaRecord> digitalMediaRecords) {
    log.debug("Successfully indexed {} digital media", digitalMediaRecords);
    var failedRecords = new HashSet<UpdatedDigitalMediaRecord>();
    for (var digitalMediaRecord : digitalMediaRecords) {
      var successfullyPublished = publishUpdateEvent(digitalMediaRecord);
      if (!successfullyPublished) {
        failedRecords.add(digitalMediaRecord);
      }
    }
    digitalMediaRecords.removeAll(failedRecords);
  }

  private boolean publishUpdateEvent(UpdatedDigitalMediaRecord digitalMediaRecord) {
    try {
      kafkaService.publishUpdateEvent(digitalMediaRecord.digitalMediaObjectRecord(),
          digitalMediaRecord.currentDigitalMediaRecord());
      return true;
    } catch (JsonProcessingException e) {
      log.error("Rolling back, failed to publish update event", e);
      rollbackUpdatedDigitalMedia(digitalMediaRecord, true);
      return false;
    }
  }

  private void rollbackUpdatedDigitalMedia(UpdatedDigitalMediaRecord updatedDigitalMediaRecord,
      boolean elasticRollback) {
    if (elasticRollback) {
      try {
        elasticRepository.rollbackVersion(updatedDigitalMediaRecord.currentDigitalMediaRecord());
      } catch (IOException e) {
        log.error("Fatal exception, unable to roll back update for: "
            + updatedDigitalMediaRecord.currentDigitalMediaRecord(), e);
      }
    }
    rollBackToEarlierVersion(updatedDigitalMediaRecord.currentDigitalMediaRecord());
    if (handleNeedsUpdate(
        updatedDigitalMediaRecord.currentDigitalMediaRecord().digitalMediaObject(),
        updatedDigitalMediaRecord.digitalMediaObjectRecord().digitalMediaObject())) {
      handleService.deleteVersion(updatedDigitalMediaRecord.currentDigitalMediaRecord());
    }
    try {
      kafkaService.deadLetterEvent(
          new DigitalMediaObjectTransferEvent(updatedDigitalMediaRecord.automatedAnnotations(),
              new DigitalMediaObjectTransfer(
                  updatedDigitalMediaRecord.digitalMediaObjectRecord().digitalMediaObject().type(),
                  updatedDigitalMediaRecord.digitalMediaObjectRecord().digitalMediaObject()
                      .physicalSpecimenId(),
                  updatedDigitalMediaRecord.digitalMediaObjectRecord().digitalMediaObject()
                      .attributes(),
                  updatedDigitalMediaRecord.digitalMediaObjectRecord().digitalMediaObject()
                      .originalAttributes()
              )));
    } catch (JsonProcessingException e) {
      log.error("Fatal exception, unable to dead letter queue: "
          + updatedDigitalMediaRecord.digitalMediaObjectRecord().id(), e);
    }
  }

  private void rollBackToEarlierVersion(DigitalMediaObjectRecord currentDigitalMedia) {
    repository.createDigitalMediaRecord(List.of(currentDigitalMedia));
  }

  private Set<UpdatedDigitalMediaRecord> getDigitalMediaRecordMap(
      List<UpdatedDigitalMediaTuple> updatedDigitalSpecimenTuples) {
    return updatedDigitalSpecimenTuples.stream().map(tuple -> new UpdatedDigitalMediaRecord(
        new DigitalMediaObjectRecord(
            tuple.currentDigitalMediaRecord().id(),
            tuple.currentDigitalMediaRecord().version() + 1,
            Instant.now(),
            tuple.digitalMediaObjectEvent().digitalMediaObject()),
        tuple.digitalMediaObjectEvent().enrichmentList(),
        tuple.currentDigitalMediaRecord()
    )).collect(Collectors.toSet());
  }

  public void updateHandles(List<UpdatedDigitalMediaTuple> updatedDigitalSpecimenTuples) {
    var handleUpdates = updatedDigitalSpecimenTuples.stream().filter(
        tuple -> handleNeedsUpdate(tuple.currentDigitalMediaRecord().digitalMediaObject(),
            tuple.digitalMediaObjectEvent().digitalMediaObject())).toList();
    if (!handleUpdates.isEmpty()) {
      handleService.updateHandles(handleUpdates);
    }
  }

  private boolean handleNeedsUpdate(DigitalMediaObject currentDigitalMediaObject,
      DigitalMediaObject digitalMediaObject) {
    return !currentDigitalMediaObject.type().equals(digitalMediaObject.type());
  }

  private void processEqualDigitalMedia(List<DigitalMediaObjectRecord> currentDigitalMediaObject) {
    var currentIds = currentDigitalMediaObject.stream().map(DigitalMediaObjectRecord::id).toList();
    repository.updateLastChecked(currentIds);
    log.info("Successfully updated lastChecked for {} existing digitalSpecimen",
        currentIds.size());
  }

  private Set<DigitalMediaObjectRecord> persistNewDigitalMediaObject(
      List<DigitalMediaObjectEvent> newRecords) {
    var digitalMediaRecords = newRecords.stream().collect(toMap(
        this::mapToDigitalMediaRecord,
        DigitalMediaObjectEvent::enrichmentList
    ));
    digitalMediaRecords.remove(null);
    if (digitalMediaRecords.isEmpty()) {
      return Collections.emptySet();
    }
    repository.createDigitalMediaRecord(digitalMediaRecords.keySet());
    log.info("{} digitalMediaObjects has been successfully committed to database",
        newRecords.size());
    try {
      var bulkResponse = elasticRepository.indexDigitalMediaObject(digitalMediaRecords.keySet());
      if (!bulkResponse.errors()) {
        handleSuccessfulElasticInsert(digitalMediaRecords);
      } else {
        handlePartiallyFailedElasticInsert(digitalMediaRecords, bulkResponse);
      }
      log.info("Successfully created {} new digital media", digitalMediaRecords.size());
      return digitalMediaRecords.keySet();
    } catch (IOException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      digitalMediaRecords.forEach(this::rollbackNewDigitalMedia);
      return Collections.emptySet();
    }
  }

  private void handlePartiallyFailedElasticInsert(
      Map<DigitalMediaObjectRecord, List<String>> digitalMediaRecords, BulkResponse bulkResponse) {
    var digitalSpecimenMap = digitalMediaRecords.keySet().stream()
        .collect(Collectors.toMap(DigitalMediaObjectRecord::id, Function.identity()));
    bulkResponse.items().forEach(
        item -> {
          var digitalSpecimenRecord = digitalSpecimenMap.get(item.id());
          if (item.error() != null) {
            log.error("Failed item to insert into elastic search: {} with errors {}",
                digitalSpecimenRecord.id(), item.error().reason());
            rollbackNewDigitalMedia(digitalSpecimenRecord,
                digitalMediaRecords.get(digitalSpecimenRecord));
            digitalMediaRecords.remove(digitalSpecimenRecord);
          } else {
            var successfullyPublished = publishEvents(digitalSpecimenRecord,
                digitalMediaRecords.get(digitalSpecimenRecord));
            if (!successfullyPublished) {
              digitalMediaRecords.remove(digitalSpecimenRecord);
            }
          }
        }
    );
  }

  private void handleSuccessfulElasticInsert(
      Map<DigitalMediaObjectRecord, List<String>> digitalMediaRecords) {
    log.debug("Successfully indexed {} specimens", digitalMediaRecords);
    for (var entry : digitalMediaRecords.entrySet()) {
      var successfullyPublished = publishEvents(entry.getKey(), entry.getValue());
      if (!successfullyPublished) {
        digitalMediaRecords.remove(entry.getKey());
      }
    }
  }

  private boolean publishEvents(DigitalMediaObjectRecord key, List<String> value) {
    try {
      kafkaService.publishCreateEvent(key);
    } catch (JsonProcessingException e) {
      log.error("Rolling back, failed to publish Create event", e);
      rollbackNewDigitalMedia(key, value, true);
      return false;
    }
    value.forEach(aas -> {
      try {
        kafkaService.publishAnnotationRequestEvent(aas, key);
      } catch (JsonProcessingException e) {
        log.error(
            "No action taken, failed to publish annotation request event for aas: {} digital media: {}",
            aas, key.id(), e);
      }
    });
    return true;
  }

  private void rollbackNewDigitalMedia(DigitalMediaObjectRecord digitalMediaObjectRecord,
      List<String> automatedAnnotations) {
    rollbackNewDigitalMedia(digitalMediaObjectRecord, automatedAnnotations, false);
  }

  private void rollbackNewDigitalMedia(DigitalMediaObjectRecord digitalMediaObjectRecord,
      List<String> automatedAnnotations, boolean elasticRollback) {

    if (elasticRollback) {
      try {
        elasticRepository.rollbackSpecimen(digitalMediaObjectRecord);
      } catch (IOException e) {
        log.error("Fatal exception, unable to roll back: " + digitalMediaObjectRecord.id(), e);
      }
    }
    repository.rollBackDigitalMedia(digitalMediaObjectRecord.id());
    handleService.rollbackHandleCreation(digitalMediaObjectRecord);
    try {
      kafkaService.deadLetterEvent(
          new DigitalMediaObjectTransferEvent(automatedAnnotations,
              new DigitalMediaObjectTransfer(
                  digitalMediaObjectRecord.digitalMediaObject().type(),
                  digitalMediaObjectRecord.digitalMediaObject().physicalSpecimenId(),
                  digitalMediaObjectRecord.digitalMediaObject().attributes(),
                  digitalMediaObjectRecord.digitalMediaObject().originalAttributes()
              )));
    } catch (JsonProcessingException e) {
      log.error("Fatal exception, unable to dead letter queue: " + digitalMediaObjectRecord.id(),
          e);
    }
  }

  private DigitalMediaObjectRecord mapToDigitalMediaRecord(DigitalMediaObjectEvent event) {
    try {
      return new DigitalMediaObjectRecord(
          handleService.createNewHandle(event.digitalMediaObject()),
          1,
          Instant.now(),
          event.digitalMediaObject()
      );
    } catch (TransformerException ex) {
      log.error("Failed to process record with ds id: {} and mediaUrl: {}",
          event.digitalMediaObject().digitalSpecimenId(),
          getMediaUrl(event.digitalMediaObject().attributes()),
          ex);
      return null;
    }
  }

  private Map<String, String> retrieveDigitalSpecimenId(
      List<String> physicalSpecimenIds) {
    return digitalSpecimenRepository.getSpecimenId(physicalSpecimenIds);
  }
}
