package eu.dissco.core.digitalmediaobjectprocessor.service;

import static java.util.stream.Collectors.toMap;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalmediaobjectprocessor.Profiles;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObject;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectEvent;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectKey;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectRecord;
import eu.dissco.core.digitalmediaobjectprocessor.domain.ProcessResult;
import eu.dissco.core.digitalmediaobjectprocessor.domain.UpdatedDigitalMediaRecord;
import eu.dissco.core.digitalmediaobjectprocessor.domain.UpdatedDigitalMediaTuple;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.DigitalSpecimenNotFoundException;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.PidCreationException;
import eu.dissco.core.digitalmediaobjectprocessor.repository.DigitalMediaObjectRepository;
import eu.dissco.core.digitalmediaobjectprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalmediaobjectprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalmediaobjectprocessor.web.HandleComponent;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessingService {

  private final DigitalMediaObjectRepository repository;
  private final FdoRecordService fdoRecordService;
  private final HandleComponent handleComponent;
  private final ElasticSearchRepository elasticRepository;
  private final KafkaPublisherService kafkaService;
  private final DigitalSpecimenRepository digitalSpecimenRepository;
  private final Environment environment;

  public List<DigitalMediaObjectRecord> handleMessage(List<DigitalMediaObjectEvent> events)
      throws DigitalSpecimenNotFoundException {
    if (environment.matchesProfiles(Profiles.WEB)) {
      checkIfDigitalSpecimenIdExists(events);
    }
    log.info("Processing {} digital media objects", events.size());
    var uniqueBatch = removeDuplicatesInBatch(events);
    var processResult = processDigitalMedia(uniqueBatch);
    var results = new ArrayList<DigitalMediaObjectRecord>();
    if (!processResult.equalMediaObjects().isEmpty()) {
      processEqualDigitalMedia(processResult.equalMediaObjects());
    }
    if (!processResult.newMediaObjects().isEmpty()) {
      results.addAll(persistNewDigitalMediaObject(processResult.newMediaObjects()));
    }
    if (!processResult.changedMediaObjects().isEmpty()) {
      results.addAll(updateExistingDigitalMedia(processResult.changedMediaObjects()));
    }
    return results;
  }

  private void checkIfDigitalSpecimenIdExists(List<DigitalMediaObjectEvent> events)
      throws DigitalSpecimenNotFoundException {
    var digitalSpecimenIds = events.stream().map(DigitalMediaObjectEvent::digitalMediaObject)
        .map(DigitalMediaObject::digitalSpecimenId).toList();
    var existingDigitalSpecimenIds = digitalSpecimenRepository.getExistingSpecimen(
        digitalSpecimenIds);
    if (!digitalSpecimenIds.equals(existingDigitalSpecimenIds)) {
      var nonExistingIds = new ArrayList<>(digitalSpecimenIds);
      nonExistingIds.removeAll(existingDigitalSpecimenIds);
      log.error("Digital specimen ids {} do not exist in database", nonExistingIds);
      throw new DigitalSpecimenNotFoundException(
          "Digital specimen ids: " + nonExistingIds + " do not exist in database");
    }
  }

  private Set<DigitalMediaObjectEvent> removeDuplicatesInBatch(
      List<DigitalMediaObjectEvent> events) {
    var uniqueSet = new LinkedHashSet<DigitalMediaObjectEvent>();
    var map = events.stream()
        .collect(
            Collectors.groupingBy(
                event -> event.digitalMediaObject().attributes().getAcAccessUri()));
    for (Entry<String, List<DigitalMediaObjectEvent>> entry : map.entrySet()) {
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

  private void republishEvent(DigitalMediaObjectEvent event) {
    try {
      kafkaService.republishDigitalMediaObject(event);
    } catch (JsonProcessingException e) {
      log.error("Fatal exception, unable to republish message due to invalid json", e);
    }
  }

  private ProcessResult processDigitalMedia(Set<DigitalMediaObjectEvent> events) {
    var currentMediaObjects = getCurrentDigitalMedia(
        events.stream().map(DigitalMediaObjectEvent::digitalMediaObject).toList());
    var equalMediaObjects = new ArrayList<DigitalMediaObjectRecord>();
    var changedMediaObjects = new ArrayList<UpdatedDigitalMediaTuple>();
    var newMediaObjects = new ArrayList<DigitalMediaObjectEvent>();

    for (var mediaObject : events) {
      var digitalMediaObject = mediaObject.digitalMediaObject();
      var digitalMediaObjectKey = new DigitalMediaObjectKey(
          digitalMediaObject.digitalSpecimenId(), digitalMediaObject.attributes().getAcAccessUri());
      log.debug("Processing digitalMediaObject: {}", digitalMediaObject);
      if (!currentMediaObjects.containsKey(digitalMediaObjectKey)) {
        log.debug("DigitalMedia with key: {} is completely new", digitalMediaObjectKey);
        newMediaObjects.add(mediaObject);
      } else {
        var currentDigitalMediaObject = currentMediaObjects.get(digitalMediaObjectKey);
        if (currentDigitalMediaObject.digitalMediaObject().equals(digitalMediaObject)) {
          log.debug("Received digital media is equal to digital media: {}",
              currentDigitalMediaObject.id());
          equalMediaObjects.add(currentDigitalMediaObject);
        } else {
          log.debug("Digital Media Object with id: {} has received an update",
              currentDigitalMediaObject.id());
          changedMediaObjects.add(
              new UpdatedDigitalMediaTuple(currentDigitalMediaObject, mediaObject));
        }
      }
    }
    return new ProcessResult(equalMediaObjects, changedMediaObjects, newMediaObjects);
  }

  private Map<DigitalMediaObjectKey, DigitalMediaObjectRecord> getCurrentDigitalMedia(
      List<DigitalMediaObject> digitalMediaObjects) {
    return repository.getDigitalMediaObject(
            digitalMediaObjects.stream().map(DigitalMediaObject::digitalSpecimenId).toList(),
            digitalMediaObjects.stream().map(digitalMedia -> digitalMedia.attributes().getAcAccessUri())
                .toList())
        .stream().collect(
            toMap(digitalMediaRecord ->
                    new DigitalMediaObjectKey(
                        digitalMediaRecord.digitalMediaObject().digitalSpecimenId(),
                        digitalMediaRecord.digitalMediaObject().attributes().getAcAccessUri()
                    ),
                Function.identity(),
                (dm1, dm2) -> {
                  log.warn("Duplicate keys found: {} and {}", dm1, dm2);
                  return dm1;
                }));
  }

  private Set<DigitalMediaObjectRecord> updateExistingDigitalMedia(
      List<UpdatedDigitalMediaTuple> updatedDigitalSpecimenTuples) {
    var digitalMediaRecords = getDigitalMediaRecordMap(updatedDigitalSpecimenTuples);
    try {
      updateHandles(digitalMediaRecords);
    } catch (PidCreationException e) {
      log.error("unable to update handle records for given request", e);
      dlqBatchUpdate(digitalMediaRecords);
      return Set.of();
    }

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
      log.info("Successfully updated {} digital media object", successfullyProcessedRecords.size());
      return successfullyProcessedRecords;
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      digitalMediaRecords.forEach(
          updatedDigitalMediaObjectRecord -> rollbackUpdatedDigitalMedia(
              updatedDigitalMediaObjectRecord,
              false));
      filterUpdatesAndRollbackHandles(new ArrayList<>(digitalMediaRecords));
      return Set.of();
    }
  }

  private void dlqBatchUpdate(Set<UpdatedDigitalMediaRecord> recordsToDlq) {
    for (var media : recordsToDlq) {
      try {
        kafkaService.deadLetterEvent(
            new DigitalMediaObjectEvent(media.automatedAnnotations(),
                new DigitalMediaObject(
                    media.digitalMediaObjectRecord().digitalMediaObject().type(),
                    media.digitalMediaObjectRecord().digitalMediaObject().digitalSpecimenId(),
                    media.digitalMediaObjectRecord().digitalMediaObject().attributes(),
                    media.digitalMediaObjectRecord().digitalMediaObject().originalAttributes()
                )));
      } catch (JsonProcessingException e) {
        log.error("Fatal exception, unable to dead letter queue media with url {} for specimen {}",
            media.digitalMediaObjectRecord().digitalMediaObject().attributes().getAcAccessUri(),
            media.digitalMediaObjectRecord().digitalMediaObject().digitalSpecimenId(),
            e);
      }
    }
  }

  private void dlqBatchCreate(List<DigitalMediaObjectEvent> recordsToDlq) {
    for (var media : recordsToDlq) {
      try {
        kafkaService.deadLetterEvent(
            new DigitalMediaObjectEvent(media.enrichmentList(),
                new DigitalMediaObject(
                    media.digitalMediaObject().type(),
                    media.digitalMediaObject().digitalSpecimenId(),
                    media.digitalMediaObject().attributes(),
                    media.digitalMediaObject().originalAttributes()
                )));
      } catch (JsonProcessingException e) {
        log.error("Fatal exception, unable to dead letter queue media with url {} for specimen {}",
            media.digitalMediaObject().attributes().getAcAccessUri(),
            media.digitalMediaObject().digitalSpecimenId(),
            e);
      }
    }
  }

  private void handlePartiallyElasticUpdate(Set<UpdatedDigitalMediaRecord> digitalMediaRecords,
      BulkResponse bulkResponse) {
    var digitalMediaMap = digitalMediaRecords.stream()
        .collect(Collectors.toMap(
            updatedDigitalMediaRecord -> updatedDigitalMediaRecord.digitalMediaObjectRecord()
                .id(), Function.identity()));
    List<UpdatedDigitalMediaRecord> handlesToRollback = new ArrayList<>();
    bulkResponse.items().forEach(
        item -> {
          var digitalMediaRecord = digitalMediaMap.get(item.id());
          if (item.error() != null) {
            log.error("Failed item to insert into elastic search: {} with errors {}",
                digitalMediaRecord.digitalMediaObjectRecord().id(), item.error().reason());
            rollbackUpdatedDigitalMedia(digitalMediaRecord, false);
            handlesToRollback.add(digitalMediaRecord);
            digitalMediaRecords.remove(digitalMediaRecord);
          } else {
            var successfullyPublished = publishUpdateEvent(digitalMediaRecord);
            if (!successfullyPublished) {
              handlesToRollback.add(digitalMediaRecord);
              digitalMediaRecords.remove(digitalMediaRecord);
            }
          }
        }
    );
    filterUpdatesAndRollbackHandles(handlesToRollback);
  }

  private void filterUpdatesAndRollbackHandles(List<UpdatedDigitalMediaRecord> handlesToRollback) {
    var recordsToRollback = handlesToRollback.stream()
        .filter(r -> fdoRecordService.handleNeedsUpdate(
            r.currentDigitalMediaRecord().digitalMediaObject(),
            r.currentDigitalMediaRecord().digitalMediaObject()))
        .toList();

    if (!recordsToRollback.isEmpty()) {
      rollbackHandleUpdate(recordsToRollback);
    }
  }

  private void rollbackHandleUpdate(List<UpdatedDigitalMediaRecord> recordsToRollback) {
    var rollbackToVersion = recordsToRollback.stream()
        .map(UpdatedDigitalMediaRecord::currentDigitalMediaRecord).toList();
    try {
      var requestBody = fdoRecordService.buildPatchDeleteRequest(rollbackToVersion);
      handleComponent.rollbackHandleUpdate(requestBody);
    } catch (PidCreationException e) {
      var handles = recordsToRollback.stream().map(media -> media.digitalMediaObjectRecord().id())
          .toList();
      log.error(
          "Unable to rollback handle updates. Handles: {}. Revert handles to the following records {}",
          handles, recordsToRollback);
    }
  }

  private void rollbackHandleCreation(List<DigitalMediaObjectRecord> recordsToRollback) {
    var requestBody = fdoRecordService.buildRollbackCreationRequest(recordsToRollback);
    try {
      handleComponent.rollbackHandleCreation(requestBody);
    } catch (PidCreationException e) {
      var ids = recordsToRollback.stream().map(DigitalMediaObjectRecord::id).toList();
      log.error("Unable to rollback handle creation. Manually delete the following handles: {} ",
          ids);
    }
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
    if (!failedRecords.isEmpty()) {
      filterUpdatesAndRollbackHandles(new ArrayList<>(failedRecords));
      digitalMediaRecords.removeAll(failedRecords);
    }

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
      } catch (IOException | ElasticsearchException e) {
        log.error("Fatal exception, unable to roll back update for: "
            + updatedDigitalMediaRecord.currentDigitalMediaRecord(), e);
      }
    }
    rollBackToEarlierVersion(updatedDigitalMediaRecord.currentDigitalMediaRecord());
    try {
      kafkaService.deadLetterEvent(
          new DigitalMediaObjectEvent(updatedDigitalMediaRecord.automatedAnnotations(),
              new DigitalMediaObject(
                  updatedDigitalMediaRecord.digitalMediaObjectRecord().digitalMediaObject().type(),
                  updatedDigitalMediaRecord.digitalMediaObjectRecord().digitalMediaObject()
                      .digitalSpecimenId(),
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
      List<UpdatedDigitalMediaTuple> updatedDigitalMediaTuples) {
    return updatedDigitalMediaTuples.stream().map(tuple -> new UpdatedDigitalMediaRecord(
        new DigitalMediaObjectRecord(
            tuple.currentDigitalMediaRecord().id(),
            tuple.currentDigitalMediaRecord().version() + 1,
            Instant.now(),
            tuple.digitalMediaObjectEvent().digitalMediaObject()),
        tuple.digitalMediaObjectEvent().enrichmentList(),
        tuple.currentDigitalMediaRecord()
    )).collect(Collectors.toSet());
  }

  public void updateHandles(Set<UpdatedDigitalMediaRecord> updatedDigitalMediaRecords)
      throws PidCreationException {
    var handleUpdates = updatedDigitalMediaRecords.stream().filter(
            pair -> fdoRecordService.handleNeedsUpdate(
                pair.currentDigitalMediaRecord().digitalMediaObject(),
                pair.digitalMediaObjectRecord().digitalMediaObject()))
        .map(UpdatedDigitalMediaRecord::digitalMediaObjectRecord)
        .toList();
    if (!handleUpdates.isEmpty()) {
      var request = fdoRecordService.buildPatchDeleteRequest(handleUpdates);
      handleComponent.updateHandle(request);
    }
  }

  private void processEqualDigitalMedia(List<DigitalMediaObjectRecord> currentDigitalMediaObject) {
    var currentIds = currentDigitalMediaObject.stream().map(DigitalMediaObjectRecord::id).toList();
    repository.updateLastChecked(currentIds);
    log.info("Successfully updated lastChecked for {} existing digital media object",
        currentIds.size());
  }

  private Set<DigitalMediaObjectRecord> persistNewDigitalMediaObject(
      List<DigitalMediaObjectEvent> newRecords) {
    var newRecordList = newRecords.stream().map(DigitalMediaObjectEvent::digitalMediaObject)
        .toList();
    Map<DigitalMediaObjectKey, String> pidMap;
    try {
      var requestBody = fdoRecordService.buildPostHandleRequest(newRecordList);
      pidMap = handleComponent.postHandle(requestBody);
    } catch (PidCreationException e) {
      log.error("Unable to create pids for given request", e);
      dlqBatchCreate(newRecords);
      return Collections.emptySet();
    }
    var digitalMediaRecords = newRecords.stream()
        .collect(toMap(event -> mapToDigitalMediaRecord(event, pidMap),
            DigitalMediaObjectEvent::enrichmentList));

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
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      digitalMediaRecords.forEach(this::rollbackNewDigitalMedia);
      var mediaRecords = digitalMediaRecords.keySet().stream().toList();
      rollbackHandleCreation(mediaRecords);
      return Collections.emptySet();
    }
  }

  private void handlePartiallyFailedElasticInsert(
      Map<DigitalMediaObjectRecord, List<String>> digitalMediaRecords, BulkResponse bulkResponse) {
    var digitalMediaMap = digitalMediaRecords.keySet().stream()
        .collect(Collectors.toMap(DigitalMediaObjectRecord::id, Function.identity()));
    var recordsToRollback = new ArrayList<DigitalMediaObjectRecord>();
    bulkResponse.items().forEach(
        item -> {
          var digitalMediaRecord = digitalMediaMap.get(item.id());
          if (item.error() != null) {
            log.error("Failed item to insert into elastic search: {} with errors {}",
                digitalMediaRecord.id(), item.error().reason());
            rollbackNewDigitalMedia(digitalMediaRecord,
                digitalMediaRecords.get(digitalMediaRecord));
            digitalMediaRecords.remove(digitalMediaRecord);
            recordsToRollback.add(digitalMediaRecord);
          } else {
            var successfullyPublished = publishEvents(digitalMediaRecord,
                digitalMediaRecords.get(digitalMediaRecord));
            if (!successfullyPublished) {
              digitalMediaRecords.remove(digitalMediaRecord);
              recordsToRollback.add(digitalMediaRecord);
            }
          }
        }
    );
    if (!recordsToRollback.isEmpty()) {
      rollbackHandleCreation(recordsToRollback);
    }
  }

  private void handleSuccessfulElasticInsert(
      Map<DigitalMediaObjectRecord, List<String>> digitalMediaRecords) {
    log.debug("Successfully indexed {} digital media", digitalMediaRecords);
    var recordsToRollback = new ArrayList<DigitalMediaObjectRecord>();
    for (var entry : digitalMediaRecords.entrySet()) {
      var successfullyPublished = publishEvents(entry.getKey(), entry.getValue());
      if (!successfullyPublished) {
        recordsToRollback.add(entry.getKey());
        digitalMediaRecords.remove(entry.getKey());
      }
    }
    if (!recordsToRollback.isEmpty()) {
      rollbackHandleCreation(recordsToRollback);
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
    value.forEach(mas -> {
      try {
        kafkaService.publishAnnotationRequestEvent(mas, key);
      } catch (JsonProcessingException e) {
        log.error(
            "No action taken, failed to publish annotation request event for aas: {} digital media: {}",
            mas, key.id(), e);
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
        elasticRepository.rollbackDigitalMedia(digitalMediaObjectRecord);
      } catch (IOException | ElasticsearchException e) {
        log.error("Fatal exception, unable to roll back: " + digitalMediaObjectRecord.id(), e);
      }
    }
    repository.rollBackDigitalMedia(digitalMediaObjectRecord.id());
    try {
      kafkaService.deadLetterEvent(
          new DigitalMediaObjectEvent(automatedAnnotations,
              new DigitalMediaObject(
                  digitalMediaObjectRecord.digitalMediaObject().type(),
                  digitalMediaObjectRecord.digitalMediaObject().digitalSpecimenId(),
                  digitalMediaObjectRecord.digitalMediaObject().attributes(),
                  digitalMediaObjectRecord.digitalMediaObject().originalAttributes()
              )));
    } catch (JsonProcessingException e) {
      log.error("Fatal exception, unable to dead letter queue: " + digitalMediaObjectRecord.id(),
          e);
    }
  }

  private DigitalMediaObjectRecord mapToDigitalMediaRecord(DigitalMediaObjectEvent event,
      Map<DigitalMediaObjectKey, String> pidMap) {
    var targetKey = new DigitalMediaObjectKey(event.digitalMediaObject().digitalSpecimenId(),
        event.digitalMediaObject().attributes().getAcAccessUri());
    var handle = pidMap.get(targetKey);
    if (handle == null) {
      log.error("Failed to process record with ds id: {} and mediaUrl: {}",
          event.digitalMediaObject().digitalSpecimenId(),
          event.digitalMediaObject().attributes().getAcAccessUri());
      return null;
    }
    return new DigitalMediaObjectRecord(
        handle,
        1,
        Instant.now(),
        event.digitalMediaObject()
    );

  }
}
