package eu.dissco.core.digitalmediaobjectprocessor.service;

import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.AAS;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.CREATED;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.DIGITAL_SPECIMEN_ID;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.DIGITAL_SPECIMEN_ID_2;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.DIGITAL_SPECIMEN_ID_3;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.FORMAT;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.FORMAT_2;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.HANDLE;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.HANDLE_2;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.HANDLE_3;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MEDIA_URL_1;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MEDIA_URL_2;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MEDIA_URL_3;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.PHYSICAL_SPECIMEN_ID_2;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.PHYSICAL_SPECIMEN_ID_3;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.TYPE;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenDigitalMediaObject;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenDigitalMediaObjectRecord;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenDigitalMediaObjectRecordPhysical;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenDigitalMediaObjectRecordWithVersion;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenDigitalMediaObjectTransferEvent;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenPidMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectKey;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectRecord;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectTransfer;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectTransferEvent;
import eu.dissco.core.digitalmediaobjectprocessor.domain.UpdatedDigitalMediaRecord;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.DigitalSpecimenNotFoundException;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.PidAuthenticationException;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.PidCreationException;
import eu.dissco.core.digitalmediaobjectprocessor.repository.DigitalMediaObjectRepository;
import eu.dissco.core.digitalmediaobjectprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalmediaobjectprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalmediaobjectprocessor.web.HandleComponent;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProcessingServiceTest {

  @Mock
  private DigitalMediaObjectRepository repository;
  @Mock
  private DigitalSpecimenRepository specimenRepository;
  @Mock
  private ElasticSearchRepository elasticRepository;
  @Mock
  private BulkResponse bulkResponse;
  @Mock
  private KafkaPublisherService publisherService;
  @Mock
  private FdoRecordService fdoRecordService;
  @Mock
  private HandleComponent handleComponent;

  private MockedStatic<Instant> mockedInstant;
  private MockedStatic<Clock> mockedClock;

  private ProcessingService service;

  @BeforeEach
  void setup() {
    service = new ProcessingService(repository, specimenRepository, fdoRecordService,
        handleComponent, elasticRepository, publisherService);
    Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
    Instant instant = Instant.now(clock);
    mockedInstant = mockStatic(Instant.class);
    mockedInstant.when(Instant::now).thenReturn(instant);
    mockedClock = mockStatic(Clock.class);
    mockedClock.when(Clock::systemUTC).thenReturn(clock);

  }

  @AfterEach
  void destroy() {
    mockedInstant.close();
    mockedClock.close();
  }

  @Test
  void testEqualDigitalMedia() throws JsonProcessingException, DigitalSpecimenNotFoundException {
    // Given
    given(specimenRepository.getSpecimenId(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        Map.of(PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID));
    given(repository.getDigitalMediaObject(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(
        List.of(givenDigitalMediaObjectRecord()));

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaObjectTransferEvent()), false);

    // Then
    then(repository).should().updateLastChecked(List.of(HANDLE));
    assertThat(result).isEmpty();
  }

  @Test
  void testUnequalDigitalMediaNoHandleUpdate() throws Exception {
    // Given
    var expected = List.of(givenDigitalMediaObjectRecordWithVersion(2));
    given(specimenRepository.getSpecimenId(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        Map.of(PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID));
    given(repository.getDigitalMediaObject(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(
        List.of(givenDigitalMediaObjectRecord(FORMAT_2)));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalMediaObject(expected)).willReturn(bulkResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(false);

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaObjectTransferEvent()), false);

    // Then
    then(handleComponent).shouldHaveNoInteractions();
    then(repository).should().createDigitalMediaRecord(expected);
    then(publisherService).should()
        .publishUpdateEvent(givenDigitalMediaObjectRecordWithVersion(2),
            givenDigitalMediaObjectRecord(FORMAT_2));
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testUnequalDigitalMediaHandleUpdate() throws Exception {
    // Given
    var expected = List.of(givenDigitalMediaObjectRecordWithVersion(2));
    given(specimenRepository.getSpecimenId(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        Map.of(PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID));
    given(repository.getDigitalMediaObject(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(
        List.of(givenDigitalMediaObjectRecord(FORMAT_2)));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalMediaObject(expected)).willReturn(bulkResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaObjectTransferEvent()), false);

    // Then
    then(fdoRecordService).should().buildPostHandleRequest(List.of(givenDigitalMediaObject()));
    then(handleComponent).should().postHandle(any());
    then(repository).should().createDigitalMediaRecord(expected);
    then(publisherService).should()
        .publishUpdateEvent(givenDigitalMediaObjectRecordWithVersion(2),
            givenDigitalMediaObjectRecord(FORMAT_2));
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testUnequalDigitalMediaHandleUpdateFailed() throws Exception {
    // Given
    given(specimenRepository.getSpecimenId(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        Map.of(PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID));
    given(repository.getDigitalMediaObject(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(
        List.of(givenDigitalMediaObjectRecord(FORMAT_2)));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(handleComponent.postHandle(any())).willThrow(PidCreationException.class);

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaObjectTransferEvent()), false);

    // Then
    then(fdoRecordService).should().buildPostHandleRequest(List.of(givenDigitalMediaObject()));
    then(repository).shouldHaveNoMoreInteractions();
    then(publisherService).should().deadLetterEvent(givenDlqTransferEventUpdate());
    assertThat(result).isEmpty();
  }

  @Test
  void testUnequalDigitalMediaHandleUpdateFailedKafkaFailed() throws Exception {
    // Given
    given(specimenRepository.getSpecimenId(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        Map.of(PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID));
    given(repository.getDigitalMediaObject(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(
        List.of(givenDigitalMediaObjectRecord(FORMAT_2)));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    given(handleComponent.postHandle(any())).willThrow(PidCreationException.class);
    doThrow(JsonProcessingException.class).when(publisherService)
        .deadLetterEvent(givenDigitalMediaObjectTransferEvent());

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaObjectTransferEvent()), false);

    // Then
    then(repository).shouldHaveNoMoreInteractions();
    then(handleComponent).should().postHandle(any());
    assertThat(result).isEmpty();
  }

  @Test
  void testNewDigitalMedia()
      throws Exception {
    // Given
    var expected = List.of(givenDigitalMediaObjectRecord());
    given(specimenRepository.getSpecimenId(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        Map.of(PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID));
    given(repository.getDigitalMediaObject(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willReturn(givenPidMap(1));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalMediaObject(
        Set.of(givenDigitalMediaObjectRecord()))).willReturn(bulkResponse);

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaObjectTransferEvent()), false);

    // Then
    then(fdoRecordService).should().buildPostHandleRequest(List.of(givenDigitalMediaObject()));
    then(handleComponent).should().postHandle(any());
    then(repository).should().createDigitalMediaRecord(Set.of(expected.get(0)));
    then(publisherService).should().publishCreateEvent(expected.get(0));
    then(publisherService).should().publishAnnotationRequestEvent(AAS, expected.get(0));
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testDuplicateNewDigitalMedia()
      throws Exception {
    // Given
    var expected = List.of(givenDigitalMediaObjectRecord());
    given(specimenRepository.getSpecimenId(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        Map.of(PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID));
    given(repository.getDigitalMediaObject(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willReturn(givenPidMap(1));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalMediaObject(
        Set.of(givenDigitalMediaObjectRecord()))).willReturn(bulkResponse);

    // When
    var result = service.handleMessage(
        List.of(givenDigitalMediaObjectTransferEvent(), givenDigitalMediaObjectTransferEvent()),
        false);

    // Then
    then(fdoRecordService).should().buildPostHandleRequest(List.of(givenDigitalMediaObject()));
    then(handleComponent).should().postHandle(any());
    then(repository).should().createDigitalMediaRecord(Set.of(expected.get(0)));
    then(publisherService).should().publishCreateEvent(expected.get(0));
    then(publisherService).should().publishAnnotationRequestEvent(AAS, expected.get(0));
    then(publisherService).should()
        .republishDigitalMediaObject(givenDigitalMediaObjectTransferEvent());
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testNewDigitalMediaIOException()
      throws Exception {
    // Given
    given(specimenRepository.getSpecimenId(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        Map.of(PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID));
    given(repository.getDigitalMediaObject(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willReturn(givenPidMap(1));
    given(elasticRepository.indexDigitalMediaObject(
        Set.of(givenDigitalMediaObjectRecord()))).willThrow(IOException.class);

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaObjectTransferEvent()), false);

    // Then
    then(fdoRecordService).should().buildPostHandleRequest(List.of(givenDigitalMediaObject()));
    then(fdoRecordService).should()
        .buildRollbackCreationRequest(List.of(givenDigitalMediaObjectRecord()));
    then(handleComponent).should().postHandle(any());
    then(handleComponent).should().rollbackHandleCreation(any());
    then(repository).should().createDigitalMediaRecord(Set.of(givenDigitalMediaObjectRecord()));
    then(repository).should().rollBackDigitalMedia(HANDLE);
    then(publisherService).should().deadLetterEvent(givenDigitalMediaObjectTransferEvent());
    assertThat(result).isEmpty();
  }

  @Test
  void testNewDigitalMediaIOExceptionHandleRollbackFailed()
      throws Exception {
    // Given
    given(specimenRepository.getSpecimenId(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        Map.of(PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID));
    given(repository.getDigitalMediaObject(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willReturn(givenPidMap(1));
    given(elasticRepository.indexDigitalMediaObject(
        Set.of(givenDigitalMediaObjectRecord()))).willThrow(IOException.class);
    doThrow(PidCreationException.class).when(handleComponent).rollbackHandleCreation(any());

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaObjectTransferEvent()), false);

    // Then
    then(fdoRecordService).should().buildPostHandleRequest(List.of(givenDigitalMediaObject()));
    then(handleComponent).should().postHandle(any());
    then(repository).should().createDigitalMediaRecord(Set.of(givenDigitalMediaObjectRecord()));
    then(repository).should().rollBackDigitalMedia(HANDLE);
    then(publisherService).should().deadLetterEvent(givenDigitalMediaObjectTransferEvent());
    assertThat(result).isEmpty();
  }

  @Test
  void testNewDigitalMediaPartialElasticFailed()
      throws Exception {
    // Given
    var secondEvent = givenDigitalMediaObjectTransferEvent(PHYSICAL_SPECIMEN_ID_2, MEDIA_URL_2);
    var thirdEvent = givenDigitalMediaObjectTransferEvent(PHYSICAL_SPECIMEN_ID_3, MEDIA_URL_3);
    given(specimenRepository.getSpecimenId(
        List.of(PHYSICAL_SPECIMEN_ID_3, PHYSICAL_SPECIMEN_ID, PHYSICAL_SPECIMEN_ID_2))).willReturn(
        Map.of(
            PHYSICAL_SPECIMEN_ID_3, DIGITAL_SPECIMEN_ID_3,
            PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID,
            PHYSICAL_SPECIMEN_ID_2, DIGITAL_SPECIMEN_ID_2)
    );
    given(repository.getDigitalMediaObject(anyList(), anyList())).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willReturn(givenPidMap(3));
    givenBulkResponse();
    given(elasticRepository.indexDigitalMediaObject(anySet())).willReturn(bulkResponse);

    // When
    var result = service.handleMessage(
        List.of(givenDigitalMediaObjectTransferEvent(), secondEvent, thirdEvent), false);

    // Then
    then(fdoRecordService).should().buildPostHandleRequest(List.of(
        givenDigitalMediaObject(DIGITAL_SPECIMEN_ID_3, PHYSICAL_SPECIMEN_ID_3, FORMAT, MEDIA_URL_3,
            TYPE),
        givenDigitalMediaObject(),
        givenDigitalMediaObject(DIGITAL_SPECIMEN_ID_2, PHYSICAL_SPECIMEN_ID_2, FORMAT, MEDIA_URL_2,
            TYPE)
    ));
    then(fdoRecordService).should().buildRollbackCreationRequest(List.of(
        givenDigitalMediaObjectRecordPhysical(HANDLE_2, PHYSICAL_SPECIMEN_ID_2,
            DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2, TYPE)));
    then(handleComponent).should().rollbackHandleCreation(any());
    then(repository).should().createDigitalMediaRecord(anySet());
    then(repository).should().rollBackDigitalMedia(HANDLE_2);
    then(publisherService).should().deadLetterEvent(secondEvent);
    assertThat(result).isEqualTo(
        List.of(givenDigitalMediaObjectRecordPhysical(HANDLE_3, PHYSICAL_SPECIMEN_ID_3,
                DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3, TYPE),
            givenDigitalMediaObjectRecord()));
  }

  @Test
  void testNewDigitalMediaPartialElasticFailedKafkaDlqFailed()
      throws Exception {
    // Given
    var secondEvent = givenDigitalMediaObjectTransferEvent(PHYSICAL_SPECIMEN_ID_2, MEDIA_URL_2);
    var thirdEvent = givenDigitalMediaObjectTransferEvent(PHYSICAL_SPECIMEN_ID_3, MEDIA_URL_3);
    given(specimenRepository.getSpecimenId(
        List.of(PHYSICAL_SPECIMEN_ID_3, PHYSICAL_SPECIMEN_ID, PHYSICAL_SPECIMEN_ID_2))).willReturn(
        Map.of(
            PHYSICAL_SPECIMEN_ID_3, DIGITAL_SPECIMEN_ID_3,
            PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID,
            PHYSICAL_SPECIMEN_ID_2, DIGITAL_SPECIMEN_ID_2)
    );
    var secondRecord =
        givenDigitalMediaObjectRecordPhysical(HANDLE_2, PHYSICAL_SPECIMEN_ID_2,
            DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2, TYPE);
    var thirdRecord = givenDigitalMediaObjectRecordPhysical(HANDLE_3, PHYSICAL_SPECIMEN_ID_3,
        DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3, TYPE);

    given(repository.getDigitalMediaObject(anyList(), anyList())).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willReturn(givenPidMap(3));
    givenBulkResponse();
    given(elasticRepository.indexDigitalMediaObject(anySet())).willReturn(bulkResponse);
    doNothing().doThrow(JsonProcessingException.class).when(publisherService).publishCreateEvent(givenDigitalMediaObjectRecord());
    doThrow(JsonProcessingException.class).when(publisherService).publishCreateEvent(thirdRecord);

    // When
    var result = service.handleMessage(
        List.of(givenDigitalMediaObjectTransferEvent(), secondEvent, thirdEvent), false);

    // Then
    then(fdoRecordService).should().buildPostHandleRequest(List.of(
        thirdRecord.digitalMediaObject(),
        givenDigitalMediaObject(),
        secondRecord.digitalMediaObject()
    ));
    then(fdoRecordService).should().buildRollbackCreationRequest(List.of(secondRecord, thirdRecord));
    then(handleComponent).should().rollbackHandleCreation(any());
    then(repository).should().createDigitalMediaRecord(anySet());
    then(repository).should().rollBackDigitalMedia(HANDLE_2);
    then(repository).should().rollBackDigitalMedia(HANDLE_3);
    then(elasticRepository).should().rollbackDigitalMedia(thirdRecord);
    then(publisherService).should().deadLetterEvent(secondEvent);
    then(publisherService).should().deadLetterEvent(thirdEvent);
    assertThat(result).isEqualTo(List.of(givenDigitalMediaObjectRecord()));
  }

  @Test
  void testNewDigitalMediaPartialElasticFailedHandleRollbackFailed()
      throws Exception {
    // Given
    var secondEvent = givenDigitalMediaObjectTransferEvent(PHYSICAL_SPECIMEN_ID_2, MEDIA_URL_2);
    var thirdEvent = givenDigitalMediaObjectTransferEvent(PHYSICAL_SPECIMEN_ID_3, MEDIA_URL_3);
    given(specimenRepository.getSpecimenId(
        List.of(PHYSICAL_SPECIMEN_ID_3, PHYSICAL_SPECIMEN_ID, PHYSICAL_SPECIMEN_ID_2))).willReturn(
        Map.of(
            PHYSICAL_SPECIMEN_ID_3, DIGITAL_SPECIMEN_ID_3,
            PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID,
            PHYSICAL_SPECIMEN_ID_2, DIGITAL_SPECIMEN_ID_2)
    );
    given(repository.getDigitalMediaObject(anyList(), anyList())).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willReturn(givenPidMap(3));
    givenBulkResponse();
    given(elasticRepository.indexDigitalMediaObject(anySet())).willReturn(bulkResponse);
    doThrow(PidCreationException.class).when(handleComponent).rollbackHandleCreation(any());

    // When
    var result = service.handleMessage(
        List.of(givenDigitalMediaObjectTransferEvent(), secondEvent, thirdEvent), false);

    // Then
    then(repository).should().createDigitalMediaRecord(anySet());
    then(repository).should().rollBackDigitalMedia(HANDLE_2);
    then(fdoRecordService).should().buildRollbackCreationRequest(List.of(
        givenDigitalMediaObjectRecordPhysical(HANDLE_2, PHYSICAL_SPECIMEN_ID_2,
            DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2, TYPE)));
    then(handleComponent).should().rollbackHandleCreation(any());
    then(publisherService).should().deadLetterEvent(secondEvent);
    assertThat(result).isEqualTo(
        List.of(givenDigitalMediaObjectRecordPhysical(HANDLE_3, PHYSICAL_SPECIMEN_ID_3,
                DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3, TYPE),
            givenDigitalMediaObjectRecord()));
  }

  @Test
  void testNewDigitalMediaPartialElasticFailedHandleRollbackFailedKafkaFailed()
      throws Exception {
    // Given
    var secondEvent = givenDigitalMediaObjectTransferEvent(PHYSICAL_SPECIMEN_ID_2, MEDIA_URL_2);
    var thirdEvent = givenDigitalMediaObjectTransferEvent(PHYSICAL_SPECIMEN_ID_3, MEDIA_URL_3);
    given(specimenRepository.getSpecimenId(
        List.of(PHYSICAL_SPECIMEN_ID_3, PHYSICAL_SPECIMEN_ID, PHYSICAL_SPECIMEN_ID_2))).willReturn(
        Map.of(
            PHYSICAL_SPECIMEN_ID_3, DIGITAL_SPECIMEN_ID_3,
            PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID,
            PHYSICAL_SPECIMEN_ID_2, DIGITAL_SPECIMEN_ID_2)
    );
    given(repository.getDigitalMediaObject(anyList(), anyList())).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willReturn(givenPidMap(3));
    givenBulkResponse();
    given(elasticRepository.indexDigitalMediaObject(anySet())).willReturn(bulkResponse);
    doThrow(PidCreationException.class).when(handleComponent).rollbackHandleCreation(any());
    doThrow(JsonProcessingException.class).when(publisherService).deadLetterEvent(secondEvent);

    // When
    var result = service.handleMessage(
        List.of(givenDigitalMediaObjectTransferEvent(), secondEvent, thirdEvent), false);

    // Then
    then(repository).should().createDigitalMediaRecord(anySet());
    then(repository).should().rollBackDigitalMedia(HANDLE_2);
    then(fdoRecordService).should().buildRollbackCreationRequest(List.of(
        givenDigitalMediaObjectRecordPhysical(HANDLE_2, PHYSICAL_SPECIMEN_ID_2,
            DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2, TYPE)));
    assertThat(result).isEqualTo(
        List.of(givenDigitalMediaObjectRecordPhysical(HANDLE_3, PHYSICAL_SPECIMEN_ID_3,
                DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3, TYPE),
            givenDigitalMediaObjectRecord()));
  }

  @Test
  void testNewDigitalMediaKafkaFailed()
      throws Exception {
    // Given
    var expected = List.of(givenDigitalMediaObjectRecord());
    given(specimenRepository.getSpecimenId(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        Map.of(PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID));
    given(repository.getDigitalMediaObject(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willReturn(givenPidMap(1));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalMediaObject(
        Set.of(givenDigitalMediaObjectRecord()))).willReturn(bulkResponse);
    doThrow(JsonProcessingException.class).when(publisherService).publishCreateEvent(any(
        DigitalMediaObjectRecord.class));

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaObjectTransferEvent()), false);

    // Then
    then(fdoRecordService).should().buildPostHandleRequest(List.of(givenDigitalMediaObject()));
    then(fdoRecordService).should()
        .buildRollbackCreationRequest(List.of(givenDigitalMediaObjectRecord()));
    then(handleComponent).should().rollbackHandleCreation(any());
    then(repository).should().createDigitalMediaRecord(anySet());
    then(repository).should().rollBackDigitalMedia(HANDLE);
    then(elasticRepository).should().rollbackDigitalMedia(expected.get(0));
    then(publisherService).should().deadLetterEvent(givenDigitalMediaObjectTransferEvent());
    assertThat(result).isEmpty();
  }

  @Test
  void testUpdateDigitalMediaPartialElasticFailed()
      throws Exception {
    // Given
    var secondEvent = givenDigitalMediaObjectTransferEvent(PHYSICAL_SPECIMEN_ID_2, MEDIA_URL_2);
    var thirdEvent = givenDigitalMediaObjectTransferEvent(PHYSICAL_SPECIMEN_ID_3, MEDIA_URL_3);
    given(specimenRepository.getSpecimenId(
        List.of(PHYSICAL_SPECIMEN_ID_3, PHYSICAL_SPECIMEN_ID, PHYSICAL_SPECIMEN_ID_2))).willReturn(
        Map.of(
            PHYSICAL_SPECIMEN_ID_3, DIGITAL_SPECIMEN_ID_3,
            PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID,
            PHYSICAL_SPECIMEN_ID_2, DIGITAL_SPECIMEN_ID_2)
    );
    given(repository.getDigitalMediaObject(anyList(), anyList())).willReturn(List.of(
        givenDigitalMediaObjectRecordPhysical(HANDLE, PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID,
            MEDIA_URL_1, "Another Type"),
        givenDigitalMediaObjectRecordPhysical(HANDLE_2, PHYSICAL_SPECIMEN_ID_2,
            DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2, "Another Type"),
        givenDigitalMediaObjectRecordPhysical(HANDLE_3, PHYSICAL_SPECIMEN_ID_3,
            DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3, "Another Type")
    ));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    givenBulkResponse();
    given(elasticRepository.indexDigitalMediaObject(anyList())).willReturn(bulkResponse);

    // When
    var result = service.handleMessage(
        List.of(givenDigitalMediaObjectTransferEvent(), secondEvent, thirdEvent), false);

    // Then
    then(handleComponent).should().postHandle(any());
    then(fdoRecordService).should().buildRollbackUpdateRequest(List.of(
        givenDigitalMediaObjectRecordPhysical(HANDLE_2, PHYSICAL_SPECIMEN_ID_2,
            DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2, "Another Type")));
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should(times(2)).createDigitalMediaRecord(anyList());
    then(publisherService).should().deadLetterEvent(secondEvent);
    assertThat(result).hasSize(2);
  }

  @Test
  void testUpdateDigitalMediaPartialElasticFailedHandleRollbackFailed()
      throws Exception {
    // Given
    var secondEvent = givenDigitalMediaObjectTransferEvent(PHYSICAL_SPECIMEN_ID_2, MEDIA_URL_2);
    var thirdEvent = givenDigitalMediaObjectTransferEvent(PHYSICAL_SPECIMEN_ID_3, MEDIA_URL_3);
    given(specimenRepository.getSpecimenId(
        List.of(PHYSICAL_SPECIMEN_ID_3, PHYSICAL_SPECIMEN_ID, PHYSICAL_SPECIMEN_ID_2))).willReturn(
        Map.of(
            PHYSICAL_SPECIMEN_ID_3, DIGITAL_SPECIMEN_ID_3,
            PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID,
            PHYSICAL_SPECIMEN_ID_2, DIGITAL_SPECIMEN_ID_2)
    );
    given(repository.getDigitalMediaObject(anyList(), anyList())).willReturn(List.of(
        givenDigitalMediaObjectRecordPhysical(HANDLE, PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID,
            MEDIA_URL_1, "Another Type"),
        givenDigitalMediaObjectRecordPhysical(HANDLE_2, PHYSICAL_SPECIMEN_ID_2,
            DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2, "Another Type"),
        givenDigitalMediaObjectRecordPhysical(HANDLE_3, PHYSICAL_SPECIMEN_ID_3,
            DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3, "Another Type")
    ));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    givenBulkResponse();
    given(elasticRepository.indexDigitalMediaObject(anyList())).willReturn(bulkResponse);
    doThrow(PidCreationException.class).when(handleComponent).rollbackHandleUpdate(any());

    // When
    var result = service.handleMessage(
        List.of(givenDigitalMediaObjectTransferEvent(), secondEvent, thirdEvent), false);

    // Then
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should(times(2)).createDigitalMediaRecord(anyList());
    then(publisherService).should().deadLetterEvent(secondEvent);
    assertThat(result).hasSize(2);
  }

  @Test
  void testUpdateDigitalMediaKafkaFailed() throws Exception {
    // Given
    var expected = List.of(givenDigitalMediaObjectRecordWithVersion(2));
    given(specimenRepository.getSpecimenId(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        Map.of(PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID));
    given(repository.getDigitalMediaObject(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(
        List.of(givenDigitalMediaObjectRecord(FORMAT_2)));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalMediaObject(expected)).willReturn(bulkResponse);
    doThrow(JsonProcessingException.class).when(publisherService)
        .publishUpdateEvent(givenDigitalMediaObjectRecordWithVersion(2),
            givenDigitalMediaObjectRecord(FORMAT_2));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaObjectTransferEvent()), false);

    // Then
    then(fdoRecordService).should().buildRollbackUpdateRequest(List.of(givenDigitalMediaObjectRecord(FORMAT_2)));
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should(times(2)).createDigitalMediaRecord(anyList());
    then(elasticRepository).should().rollbackVersion(givenDigitalMediaObjectRecord(FORMAT_2));
    then(publisherService).should().deadLetterEvent(givenDigitalMediaObjectTransferEvent());
    assertThat(result).isEmpty();
  }

  @Test
  void testUpdateDigitalMediaIOException() throws Exception {
    // Given
    var expected = List.of(givenDigitalMediaObjectRecordWithVersion(2));
    given(specimenRepository.getSpecimenId(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        Map.of(PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID));
    given(repository.getDigitalMediaObject(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(
        List.of(givenDigitalMediaObjectRecord(FORMAT_2)));
    given(elasticRepository.indexDigitalMediaObject(expected)).willThrow(IOException.class);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaObjectTransferEvent()), false);
    var a = HANDLE;

    // Then
    then(fdoRecordService).should().buildRollbackUpdateRequest(List.of(
        givenDigitalMediaObjectRecord(FORMAT_2)));
    then(repository).should(times(2)).createDigitalMediaRecord(anyList());
    then(publisherService).should().deadLetterEvent(givenDigitalMediaObjectTransferEvent());
    assertThat(result).isEmpty();
  }

  @Test
  void testNewDigitalMediaHandleException()
      throws Exception {
    // Given
    given(specimenRepository.getSpecimenId(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        Map.of(PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID));
    given(repository.getDigitalMediaObject(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willThrow(PidAuthenticationException.class);

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaObjectTransferEvent()), false);

    // Then
    then(repository).shouldHaveNoMoreInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
    then(publisherService).should().deadLetterEvent(givenDigitalMediaObjectTransferEvent());
    assertThat(result).isEmpty();
  }

  @Test
  void testNewDigitalMediaHandleExceptionKafkaFailed() throws Exception {
    // Given
    given(specimenRepository.getSpecimenId(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(
        Map.of(PHYSICAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID));
    given(repository.getDigitalMediaObject(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willThrow(PidAuthenticationException.class);
    doThrow(JsonProcessingException.class).when(publisherService).deadLetterEvent(givenDigitalMediaObjectTransferEvent());

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaObjectTransferEvent()), false);

    // Then
    then(repository).shouldHaveNoMoreInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
    assertThat(result).isEmpty();
  }

  @Test
  void testDigitalSpecimenMissingKafka()
      throws JsonProcessingException, DigitalSpecimenNotFoundException {
    // Given
    given(specimenRepository.getSpecimenId(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(Map.of());

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaObjectTransferEvent()), false);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void testDigitalSpecimenMissingWeb() {
    // Given
    given(specimenRepository.getSpecimenId(List.of(PHYSICAL_SPECIMEN_ID))).willReturn(Map.of());

    // When
    assertThatThrownBy(() -> service.handleMessage(List.of(givenDigitalMediaObjectTransferEvent()),
        true)).isInstanceOf(DigitalSpecimenNotFoundException.class);

  }

  private void givenBulkResponse() {
    var positiveResponse = mock(BulkResponseItem.class);
    given(positiveResponse.error()).willReturn(null);
    given(positiveResponse.id()).willReturn(HANDLE).willReturn(HANDLE_3);
    var negativeResponse = mock(BulkResponseItem.class);
    given(negativeResponse.error()).willReturn(new ErrorCause.Builder().reason("Crashed").build());
    given(negativeResponse.id()).willReturn(HANDLE_2);
    given(bulkResponse.errors()).willReturn(true);
    given(bulkResponse.items()).willReturn(
        List.of(positiveResponse, negativeResponse, positiveResponse));
  }

  private DigitalMediaObjectTransferEvent givenDlqTransferEventUpdate() throws Exception {
    var mediaRecord = new UpdatedDigitalMediaRecord(
        givenDigitalMediaObjectRecord(),
        List.of(AAS),
        givenDigitalMediaObjectRecord(FORMAT_2)
    );
    return new DigitalMediaObjectTransferEvent(mediaRecord.automatedAnnotations(),
        new DigitalMediaObjectTransfer(
            mediaRecord.digitalMediaObjectRecord().digitalMediaObject().type(),
            mediaRecord.digitalMediaObjectRecord().digitalMediaObject().physicalSpecimenId(),
            mediaRecord.digitalMediaObjectRecord().digitalMediaObject().attributes(),
            mediaRecord.digitalMediaObjectRecord().digitalMediaObject().originalAttributes()
        ));
  }

}
