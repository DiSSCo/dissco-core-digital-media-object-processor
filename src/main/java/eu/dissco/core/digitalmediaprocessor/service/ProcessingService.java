package eu.dissco.core.digitalmediaprocessor.service;

import static eu.dissco.core.digitalmediaprocessor.configuration.ApplicationConfiguration.DATE_STRING;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonpatch.diff.JsonDiff;
import eu.dissco.core.digitalmediaprocessor.Profiles;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaEvent;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaKey;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaWrapper;
import eu.dissco.core.digitalmediaprocessor.domain.ProcessResult;
import eu.dissco.core.digitalmediaprocessor.domain.UpdatedDigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.domain.UpdatedDigitalMediaTuple;
import eu.dissco.core.digitalmediaprocessor.exceptions.DigitalSpecimenNotFoundException;
import eu.dissco.core.digitalmediaprocessor.exceptions.PidCreationException;
import eu.dissco.core.digitalmediaprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalmediaprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalmediaprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalmediaprocessor.schema.DigitalMedia;
import eu.dissco.core.digitalmediaprocessor.schema.EntityRelationship;
import eu.dissco.core.digitalmediaprocessor.web.HandleComponent;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.exception.DataAccessException;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessingService {

  private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_STRING)
      .withZone(ZoneOffset.UTC);

  private final ObjectMapper mapper;
  private final DigitalMediaRepository repository;
  private final FdoRecordService fdoRecordService;
  private final HandleComponent handleComponent;
  private final ElasticSearchRepository elasticRepository;
  private final KafkaPublisherService kafkaService;
  private final DigitalSpecimenRepository digitalSpecimenRepository;
  private final Environment environment;
  private final AnnotationPublisherService annotationPublisherService;

  private static DigitalMediaEvent mapUpdatedRecordToEvent(UpdatedDigitalMediaRecord media) {
    return new DigitalMediaEvent(media.automatedAnnotations(),
        new DigitalMediaWrapper(
            media.digitalMediaRecord().digitalMediaWrapper().type(),
            media.digitalMediaRecord().digitalMediaWrapper()
                .digitalSpecimenID(),
            media.digitalMediaRecord().digitalMediaWrapper().attributes(),
            media.digitalMediaRecord().digitalMediaWrapper()
                .originalAttributes()
        ));
  }

  public List<DigitalMediaRecord> handleMessage(List<DigitalMediaEvent> events)
      throws DigitalSpecimenNotFoundException {
    if (environment.matchesProfiles(Profiles.WEB)) {
      checkIfDigitalSpecimenIdExists(events);
    }
    log.info("Processing {} digital media", events.size());
    var uniqueBatch = removeDuplicatesInBatch(events);
    var processResult = processDigitalMedia(uniqueBatch);
    var results = new ArrayList<DigitalMediaRecord>();
    if (!processResult.equalDigitalMedia().isEmpty()) {
      processEqualDigitalMedia(processResult.equalDigitalMedia());
    }
    if (!processResult.newDigitalMedia().isEmpty()) {
      results.addAll(persistNewDigitalMedia(processResult.newDigitalMedia()));
    }
    if (!processResult.changedDigitalMedia().isEmpty()) {
      results.addAll(updateExistingDigitalMedia(processResult.changedDigitalMedia()));
    }
    return results;
  }

  private void checkIfDigitalSpecimenIdExists(List<DigitalMediaEvent> events)
      throws DigitalSpecimenNotFoundException {
    var digitalSpecimenIds = events.stream()
        .map(event -> event.digitalMediaWrapper().digitalSpecimenID()).collect(toSet());
    var currentDigitalSpecimenIds = digitalSpecimenRepository.getExistingSpecimen(
        digitalSpecimenIds);
    if (!digitalSpecimenIds.equals(currentDigitalSpecimenIds)) {
      var nonCurrentIds = new ArrayList<>(digitalSpecimenIds);
      nonCurrentIds.removeAll(currentDigitalSpecimenIds);
      log.error("Digital specimen ids {} do not exist in database", nonCurrentIds);
      throw new DigitalSpecimenNotFoundException(
          "Digital specimen ids: " + nonCurrentIds + " do not exist in database");
    }
  }

  private Set<DigitalMediaEvent> removeDuplicatesInBatch(
      List<DigitalMediaEvent> events) {
    var uniqueSet = new LinkedHashSet<DigitalMediaEvent>();
    var map = events.stream()
        .collect(
            Collectors.groupingBy(
                event -> event.digitalMediaWrapper().attributes().getAcAccessURI()));
    for (Entry<String, List<DigitalMediaEvent>> entry : map.entrySet()) {
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

  private void republishEvent(DigitalMediaEvent event) {
    try {
      kafkaService.republishDigitalMedia(event);
    } catch (JsonProcessingException e) {
      log.error("Fatal exception, unable to republish message due to invalid json", e);
    }
  }

  private ProcessResult processDigitalMedia(Set<DigitalMediaEvent> events) {
    var currentDigitalMedias = getCurrentDigitalMedia(
        events.stream().map(DigitalMediaEvent::digitalMediaWrapper).toList());
    var equalDigitalMedia = new ArrayList<DigitalMediaRecord>();
    var changedDigitalMedia = new ArrayList<UpdatedDigitalMediaTuple>();
    var newDigitalMedia = new ArrayList<DigitalMediaEvent>();
    for (var digitalMedia : events) {
      var digitalMediaWrapper = digitalMedia.digitalMediaWrapper();
      var digitalMediaKey = new DigitalMediaKey(
          digitalMediaWrapper.digitalSpecimenID(),
          digitalMediaWrapper.attributes().getAcAccessURI());
      log.debug("Processing digitalMediaWrapper: {}", digitalMediaWrapper);
      if (!currentDigitalMedias.containsKey(digitalMediaKey)) {
        log.debug("DigitalMedia with key: {} is completely new", digitalMediaKey);
        newDigitalMedia.add(digitalMedia);
      } else {
        var currentDigitalMedia = currentDigitalMedias.get(digitalMediaKey);
        if (isEqual(currentDigitalMedia.digitalMediaWrapper(), digitalMediaWrapper)) {
          log.debug("Received digital media is equal to digital media: {}",
              currentDigitalMedia.id());
          equalDigitalMedia.add(currentDigitalMedia);
        } else {
          log.debug("Digital Media Object with id: {} has received an update",
              currentDigitalMedia.id());
          changedDigitalMedia.add(
              new UpdatedDigitalMediaTuple(currentDigitalMedia, digitalMedia));
        }
      }
    }
    return new ProcessResult(equalDigitalMedia, changedDigitalMedia, newDigitalMedia);
  }

  /*
  We need to remove the Modified and EntityRelationshipDate from the comparison.
  We take over the ERDate from the current entity relationship if the ER is equal.

  To establish equality, we only compare type and attributes, not original data or
  physical specimen id. We ignore original data because original data is not updated
  if it does change, and we ignore physical specimen id because that's how the specimens
  were identified to be the same in the first place.
  */
  private boolean isEqual(DigitalMediaWrapper currentDigitalMediaWrapper,
      DigitalMediaWrapper digitalMediaWrapper) {
    if (currentDigitalMediaWrapper.attributes() == null) {
      return false;
    }
    var currentModified = currentDigitalMediaWrapper.attributes().getDctermsModified();
    currentDigitalMediaWrapper.attributes().setDctermsModified(null);
    digitalMediaWrapper.attributes().setDctermsModified(null);
    var entityRelationships = digitalMediaWrapper.attributes().getOdsHasEntityRelationship();
    setTimestampsEntityRelationships(entityRelationships,
        currentDigitalMediaWrapper.attributes().getOdsHasEntityRelationship());
    checkOriginalData(currentDigitalMediaWrapper, digitalMediaWrapper);
    if (currentDigitalMediaWrapper.attributes().equals(digitalMediaWrapper.attributes())
        && currentDigitalMediaWrapper.digitalSpecimenID()
        .equals(digitalMediaWrapper.digitalSpecimenID())
        && currentDigitalMediaWrapper.type().equals(digitalMediaWrapper.type())
    ) {
      digitalMediaWrapper.attributes().setDctermsModified(currentModified);
      currentDigitalMediaWrapper.attributes().setDctermsModified(currentModified);
      return true;
    } else {
      digitalMediaWrapper.attributes().setDctermsModified(formatter.format(Instant.now()));
      currentDigitalMediaWrapper.attributes().setDctermsModified(currentModified);
      return false;
    }
  }

  private void checkOriginalData(DigitalMediaWrapper currentDigitalMediaWrapper,
      DigitalMediaWrapper digitalMediaWrapper) {
    if (currentDigitalMediaWrapper.originalAttributes() != null
        && currentDigitalMediaWrapper.originalAttributes()
        .equals(digitalMediaWrapper.originalAttributes())) {
      log.info(
          "Media Object with ac:accessURI {} has changed original data. New Original data not captured.",
          digitalMediaWrapper.attributes().getAcAccessURI());
    }
  }

  /*
  When all fields are equal except the timestamp we assume tha relationships are equal and the
  timestamp can be taken over from the current entity relationship.
  This will reduce the amount of updates and will only update the ER timestamp when there was an
  actual change
  */
  private void setTimestampsEntityRelationships(List<EntityRelationship> entityRelationships,
      List<EntityRelationship> currentEntityRelationships) {
    for (var entityRelationship : entityRelationships) {
      for (var currentEntityrelationship : currentEntityRelationships) {
        if (Objects.equals(entityRelationship.getId(), currentEntityrelationship.getId()) &&
            Objects.equals(entityRelationship.getType(), currentEntityrelationship.getType()) &&
            Objects.equals(entityRelationship.getDwcRelationshipOfResource(),
                currentEntityrelationship.getDwcRelationshipOfResource()) &&
            Objects.equals(entityRelationship.getDwcRelationshipOfResourceID(),
                currentEntityrelationship.getDwcRelationshipOfResourceID()) &&
            Objects.equals(entityRelationship.getDwcRelatedResourceID(),
                currentEntityrelationship.getDwcRelatedResourceID()) &&
            Objects.equals(entityRelationship.getOdsRelatedResourceURI(),
                currentEntityrelationship.getOdsRelatedResourceURI()) &&
            Objects.equals(entityRelationship.getDwcRelationshipAccordingTo(),
                currentEntityrelationship.getDwcRelationshipAccordingTo()) &&
            Objects.equals(entityRelationship.getOdsRelationshipAccordingToAgent(),
                currentEntityrelationship.getOdsRelationshipAccordingToAgent()) &&
            Objects.equals(entityRelationship.getOdsEntityRelationshipOrder(),
                currentEntityrelationship.getOdsEntityRelationshipOrder()) &&
            Objects.equals(entityRelationship.getDwcRelationshipRemarks(),
                currentEntityrelationship.getDwcRelationshipRemarks())
        ) {
          entityRelationship.setDwcRelationshipEstablishedDate(
              currentEntityrelationship.getDwcRelationshipEstablishedDate());
        }
      }
    }
  }

  private Map<DigitalMediaKey, DigitalMediaRecord> getCurrentDigitalMedia(
      List<DigitalMediaWrapper> digitalMediaWrappers) {
    var digitalMediaWrappersWithNoIds = new ArrayList<DigitalMediaWrapper>();
    var digitalMediaWrappersWithIds = new ArrayList<DigitalMediaWrapper>();
    digitalMediaWrappers.forEach(digitalMediaWrapper -> {
      if (digitalMediaWrapper.attributes().getId() != null){
        digitalMediaWrappersWithIds.add(digitalMediaWrapper);
      } else {
        digitalMediaWrappersWithNoIds.add(digitalMediaWrapper);
      }
    });
    return repository.getDigitalMedia(
            digitalMediaWrappers.stream().map(DigitalMediaWrapper::digitalSpecimenID).toList(),
            digitalMediaWrappers.stream()
                .map(digitalMedia -> digitalMedia.attributes().getAcAccessURI())
                .toList())
        .stream().collect(
            toMap(digitalMediaRecord ->
                    new DigitalMediaKey(
                        digitalMediaRecord.digitalMediaWrapper().digitalSpecimenID(),
                        digitalMediaRecord.digitalMediaWrapper().attributes().getAcAccessURI()
                    ),
                Function.identity(),
                (dm1, dm2) -> {
                  log.warn("Duplicate keys found: {} and {}", dm1, dm2);
                  return dm1;
                }));
  }

  private Set<DigitalMediaRecord> updateExistingDigitalMedia(
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
    try {
      repository.createDigitalMediaRecord(
          digitalMediaRecords.stream().map(UpdatedDigitalMediaRecord::digitalMediaRecord)
              .toList());
    } catch (DataAccessException e) {
      log.error("Database exception: unable to post updates to db", e);
      rollbackHandleUpdate(new ArrayList<>(digitalMediaRecords));
      for (var updatedRecord : digitalMediaRecords) {
        try {
          kafkaService.deadLetterEvent(mapUpdatedRecordToEvent(updatedRecord));
        } catch (JsonProcessingException e2) {
          log.error("Fatal exception: unable to DLQ failed event", e);
        }
      }
      return Collections.emptySet();
    }
    log.info("Persisting to elastic");
    try {
      var bulkResponse = elasticRepository.indexDigitalMedia(
          digitalMediaRecords.stream().map(UpdatedDigitalMediaRecord::digitalMediaRecord)
              .toList());
      if (!bulkResponse.errors()) {
        handleSuccessfulElasticUpdate(digitalMediaRecords);
      } else {
        handlePartiallyElasticUpdate(digitalMediaRecords, bulkResponse);
      }
      var successfullyProcessedRecords = digitalMediaRecords.stream()
          .map(UpdatedDigitalMediaRecord::digitalMediaRecord).collect(
              toSet());
      log.info("Successfully updated {} digital media object", successfullyProcessedRecords.size());
      annotationPublisherService.publishAnnotationUpdatedMedia(digitalMediaRecords);
      return successfullyProcessedRecords;
    } catch (IOException | ElasticsearchException e) {
      log.error("Rolling back, failed to insert records in elastic", e);
      digitalMediaRecords.forEach(
          updatedDigitalMediaRecord -> rollbackUpdatedDigitalMedia(
              updatedDigitalMediaRecord,
              false));
      filterUpdatesAndRollbackHandles(new ArrayList<>(digitalMediaRecords));
      return Set.of();
    }
  }

  private void dlqBatchUpdate(Set<UpdatedDigitalMediaRecord> recordsToDlq) {
    for (var media : recordsToDlq) {
      try {
        kafkaService.deadLetterEvent(mapUpdatedRecordToEvent(media));
      } catch (JsonProcessingException e) {
        log.error("Fatal exception, unable to dead letter queue media with url {} for specimen {}",
            media.digitalMediaRecord().digitalMediaWrapper().attributes()
                .getAcAccessURI(),
            media.digitalMediaRecord().digitalMediaWrapper().digitalSpecimenID(),
            e);
      }
    }
  }

  private void dlqBatchCreate(List<DigitalMediaEvent> recordsToDlq) {
    for (var media : recordsToDlq) {
      try {
        kafkaService.deadLetterEvent(
            new DigitalMediaEvent(media.enrichmentList(),
                new DigitalMediaWrapper(
                    media.digitalMediaWrapper().type(),
                    media.digitalMediaWrapper().digitalSpecimenID(),
                    media.digitalMediaWrapper().attributes(),
                    media.digitalMediaWrapper().originalAttributes()
                )));
      } catch (JsonProcessingException e) {
        log.error("Fatal exception, unable to dead letter queue media with url {} for specimen {}",
            media.digitalMediaWrapper().attributes().getAcAccessURI(),
            media.digitalMediaWrapper().digitalSpecimenID(),
            e);
      }
    }
  }

  private void handlePartiallyElasticUpdate(Set<UpdatedDigitalMediaRecord> digitalMediaRecords,
      BulkResponse bulkResponse) {
    var digitalMediaMap = digitalMediaRecords.stream()
        .collect(Collectors.toMap(
            updatedDigitalMediaRecord -> updatedDigitalMediaRecord.digitalMediaRecord()
                .id(), Function.identity()));
    List<UpdatedDigitalMediaRecord> handlesToRollback = new ArrayList<>();
    bulkResponse.items().forEach(
        item -> {
          var digitalMediaRecord = digitalMediaMap.get(item.id());
          if (item.error() != null) {
            log.error("Failed item to insert into elastic search: {} with errors {}",
                digitalMediaRecord.digitalMediaRecord().id(), item.error().reason());
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
            r.currentDigitalMediaRecord().digitalMediaWrapper(),
            r.currentDigitalMediaRecord().digitalMediaWrapper()))
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
      var handles = recordsToRollback.stream().map(media -> media.digitalMediaRecord().id())
          .toList();
      log.error(
          "Unable to rollback handle updates. Handles: {}. Revert handles to the following records {}",
          handles, recordsToRollback);
    }
  }

  private void rollbackHandleCreation(List<DigitalMediaRecord> recordsToRollback) {
    var requestBody = fdoRecordService.buildRollbackCreationRequest(recordsToRollback);
    try {
      handleComponent.rollbackHandleCreation(requestBody);
    } catch (PidCreationException e) {
      var ids = recordsToRollback.stream().map(DigitalMediaRecord::id).toList();
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
      kafkaService.publishUpdateEvent(digitalMediaRecord.digitalMediaRecord(),
          digitalMediaRecord.jsonPatch());
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
          new DigitalMediaEvent(updatedDigitalMediaRecord.automatedAnnotations(),
              new DigitalMediaWrapper(
                  updatedDigitalMediaRecord.digitalMediaRecord().digitalMediaWrapper()
                      .type(),
                  updatedDigitalMediaRecord.digitalMediaRecord().digitalMediaWrapper()
                      .digitalSpecimenID(),
                  updatedDigitalMediaRecord.digitalMediaRecord().digitalMediaWrapper()
                      .attributes(),
                  updatedDigitalMediaRecord.digitalMediaRecord().digitalMediaWrapper()
                      .originalAttributes()
              )));
    } catch (JsonProcessingException e) {
      log.error("Fatal exception, unable to dead letter queue: "
          + updatedDigitalMediaRecord.digitalMediaRecord().id(), e);
    }
  }

  private void rollBackToEarlierVersion(DigitalMediaRecord currentDigitalMedia) {
    try {
      repository.createDigitalMediaRecord(List.of(currentDigitalMedia));
    } catch (DataAccessException e) {
      log.error("Database exception: Unable to rollback media to earlier version", e);
    }

  }

  private Set<UpdatedDigitalMediaRecord> getDigitalMediaRecordMap(
      List<UpdatedDigitalMediaTuple> updatedDigitalMediaTuples) {
    return updatedDigitalMediaTuples.stream().map(tuple -> new UpdatedDigitalMediaRecord(
        new DigitalMediaRecord(
            tuple.currentDigitalMediaRecord().id(),
            tuple.currentDigitalMediaRecord().version() + 1,
            Instant.now(),
            tuple.digitalMediaEvent().digitalMediaWrapper()),
        tuple.digitalMediaEvent().enrichmentList(),
        tuple.currentDigitalMediaRecord(),
        createJsonPatch(tuple.currentDigitalMediaRecord().digitalMediaWrapper().attributes(),
            tuple.digitalMediaEvent().digitalMediaWrapper().attributes())
    )).collect(toSet());
  }

  public void updateHandles(Set<UpdatedDigitalMediaRecord> updatedDigitalMediaRecords)
      throws PidCreationException {
    var handleUpdates = updatedDigitalMediaRecords.stream().filter(
            pair -> fdoRecordService.handleNeedsUpdate(
                pair.currentDigitalMediaRecord().digitalMediaWrapper(),
                pair.digitalMediaRecord().digitalMediaWrapper()))
        .map(UpdatedDigitalMediaRecord::digitalMediaRecord)
        .toList();
    if (!handleUpdates.isEmpty()) {
      var request = fdoRecordService.buildPatchDeleteRequest(handleUpdates);
      handleComponent.updateHandle(request);
    }
  }

  private void processEqualDigitalMedia(List<DigitalMediaRecord> currentDigitalMedia) {
    var currentIds = currentDigitalMedia.stream().map(DigitalMediaRecord::id).toList();
    repository.updateLastChecked(currentIds);
    log.info("Successfully updated lastChecked for {} existing digital media",
        currentIds.size());
  }

  private Set<DigitalMediaRecord> persistNewDigitalMedia(
      List<DigitalMediaEvent> newRecords) {
    var newRecordList = newRecords.stream().map(DigitalMediaEvent::digitalMediaWrapper)
        .toList();
    Map<DigitalMediaKey, String> pidMap;
    try {
      // The specimen processor sends media events with ids
      // If a request comes form a different source, we may need to mint PIDs here
      var requestBody = fdoRecordService.buildPostHandleRequest(newRecordList
          .stream()
          .filter(media -> media.attributes().getId() == null).toList());
      if (!requestBody.isEmpty()) {
        log.info("Minting {} new handles", requestBody.size());
        pidMap = handleComponent.postHandle(requestBody);
      } else {
        pidMap = Map.of();
      }
    } catch (PidCreationException e) {
      log.error("Unable to create pids for given request", e);
      dlqBatchCreate(newRecords);
      return Collections.emptySet();
    }
    var digitalMediaRecords = newRecords.stream()
        .collect(toMap(event -> mapToDigitalMediaRecord(event, pidMap),
            DigitalMediaEvent::enrichmentList));
    digitalMediaRecords.remove(null);
    if (digitalMediaRecords.isEmpty()) {
      return Collections.emptySet();
    }
    try {
      repository.createDigitalMediaRecord(digitalMediaRecords.keySet());
    } catch (DataAccessException e) {
      log.error("Database exception, unable to post new digital media to database", e);
      rollbackHandleCreation(new ArrayList<>(digitalMediaRecords.keySet()));
      for (var event : newRecords) {
        try {
          kafkaService.deadLetterEvent(event);
        } catch (JsonProcessingException e2) {
          log.error("Fatal Exception: unable to post event to DLQ", e);
        }
      }
      return Collections.emptySet();
    }

    log.info("{} digital media has been successfully committed to database",
        newRecords.size());
    try {
      var bulkResponse = elasticRepository.indexDigitalMedia(digitalMediaRecords.keySet());
      if (!bulkResponse.errors()) {
        handleSuccessfulElasticInsert(digitalMediaRecords);
      } else {
        handlePartiallyFailedElasticInsert(digitalMediaRecords, bulkResponse);
      }
      log.info("Successfully created {} new digital media", digitalMediaRecords.size());
      annotationPublisherService.publishAnnotationNewMedia(digitalMediaRecords.keySet());
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
      Map<DigitalMediaRecord, List<String>> digitalMediaRecords, BulkResponse bulkResponse) {
    var digitalMediaMap = digitalMediaRecords.keySet().stream()
        .collect(Collectors.toMap(DigitalMediaRecord::id, Function.identity()));
    var recordsToRollback = new ArrayList<DigitalMediaRecord>();
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
      Map<DigitalMediaRecord, List<String>> digitalMediaRecords) {
    log.debug("Successfully indexed {} digital media", digitalMediaRecords);
    var recordsToRollback = new ArrayList<DigitalMediaRecord>();
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

  private boolean publishEvents(DigitalMediaRecord key, List<String> value) {
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

  private void rollbackNewDigitalMedia(DigitalMediaRecord digitalMediaRecord,
      List<String> automatedAnnotations) {
    rollbackNewDigitalMedia(digitalMediaRecord, automatedAnnotations, false);
  }

  private void rollbackNewDigitalMedia(DigitalMediaRecord digitalMediaRecord,
      List<String> automatedAnnotations, boolean elasticRollback) {

    if (elasticRollback) {
      try {
        elasticRepository.rollbackDigitalMedia(digitalMediaRecord);
      } catch (IOException | ElasticsearchException e) {
        log.error("Fatal exception, unable to roll back: " + digitalMediaRecord.id(), e);
      }
    }
    repository.rollBackDigitalMedia(digitalMediaRecord.id());
    try {
      kafkaService.deadLetterEvent(
          new DigitalMediaEvent(automatedAnnotations,
              new DigitalMediaWrapper(
                  digitalMediaRecord.digitalMediaWrapper().type(),
                  digitalMediaRecord.digitalMediaWrapper().digitalSpecimenID(),
                  digitalMediaRecord.digitalMediaWrapper().attributes(),
                  digitalMediaRecord.digitalMediaWrapper().originalAttributes()
              )));
    } catch (JsonProcessingException e) {
      log.error("Fatal exception, unable to dead letter queue: " + digitalMediaRecord.id(),
          e);
    }
  }

  private DigitalMediaRecord mapToDigitalMediaRecord(DigitalMediaEvent event,
      Map<DigitalMediaKey, String> pidMap) {
    String handle;
    if (event.digitalMediaWrapper().attributes().getId() != null) {
      handle = event.digitalMediaWrapper().attributes().getId();
    } else {
      var targetKey = new DigitalMediaKey(event.digitalMediaWrapper().digitalSpecimenID(),
          event.digitalMediaWrapper().attributes().getAcAccessURI());
      handle = pidMap.get(targetKey);
      if (handle == null) {
        log.error("Failed to process record with ds id: {} and mediaUrl: {}",
            event.digitalMediaWrapper().digitalSpecimenID(),
            event.digitalMediaWrapper().attributes().getAcAccessURI());
        return null;
      }
    }
    return new DigitalMediaRecord(
        handle,
        1,
        Instant.now(),
        event.digitalMediaWrapper()
    );
  }


  private JsonNode createJsonPatch(DigitalMedia currentDigitalMedia, DigitalMedia digitalMedia) {
    var jsonCurrentMedia = (ObjectNode) mapper.valueToTree(currentDigitalMedia);
    var jsonMedia = (ObjectNode) mapper.valueToTree(digitalMedia);
    jsonCurrentMedia.set("dcterms:modified", null);
    jsonMedia.set("dcterms:modified", null);
    return JsonDiff.asJson(jsonCurrentMedia, jsonMedia);
  }
}
