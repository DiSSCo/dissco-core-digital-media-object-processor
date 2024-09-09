package eu.dissco.core.digitalmediaprocessor.service;

import static eu.dissco.core.digitalmediaprocessor.TestUtils.CREATED;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.DIGITAL_SPECIMEN_ID;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.DIGITAL_SPECIMEN_ID_2;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.DIGITAL_SPECIMEN_ID_3;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.FORMAT;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.FORMAT_2;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.HANDLE;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.HANDLE_2;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.HANDLE_3;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.MAPPER;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.MAS;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.MEDIA_URL_1;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.MEDIA_URL_2;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.MEDIA_URL_3;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.TYPE;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenDigitalMediaRecordNoOriginalData;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenDigitalMediaRecordPhysical;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenDigitalMediaRecordWithVersion;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenDigitalMediaWrapper;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenJsonPatch;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenPidMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
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
import eu.dissco.core.digitalmediaprocessor.Profiles;
import eu.dissco.core.digitalmediaprocessor.TestUtils;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaEvent;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaWrapper;
import eu.dissco.core.digitalmediaprocessor.domain.UpdatedDigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.exceptions.DigitalSpecimenNotFoundException;
import eu.dissco.core.digitalmediaprocessor.exceptions.PidCreationException;
import eu.dissco.core.digitalmediaprocessor.repository.DigitalMediaRepository;
import eu.dissco.core.digitalmediaprocessor.repository.DigitalSpecimenRepository;
import eu.dissco.core.digitalmediaprocessor.repository.ElasticSearchRepository;
import eu.dissco.core.digitalmediaprocessor.web.HandleComponent;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.env.Environment;

@ExtendWith(MockitoExtension.class)
class ProcessingServiceTest {

  @Mock
  private DigitalMediaRepository repository;
  @Mock
  private DigitalSpecimenRepository digitalSpecimenRepository;
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
  @Mock
  private Environment environment;
  @Mock
  private AnnotationPublisherService annotationPublisherService;

  private MockedStatic<Instant> mockedInstant;
  private MockedStatic<Clock> mockedClock;

  private ProcessingService service;

  @BeforeEach
  void setup() {
    service = new ProcessingService(MAPPER, repository, fdoRecordService, handleComponent,
        elasticRepository, publisherService, digitalSpecimenRepository, environment,
        annotationPublisherService);
    Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
    Instant instant = Instant.now(clock);
    mockedInstant = mockStatic(Instant.class);
    mockedInstant.when(Instant::now).thenReturn(instant);
    mockedInstant.when(() -> Instant.from(any())).thenReturn(instant);
    mockedInstant.when(() -> Instant.parse(any())).thenReturn(instant);
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
    given(repository.getDigitalMedia(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(
        List.of(givenDigitalMediaRecord()));

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaEvent()));

    // Then
    then(repository).should().updateLastChecked(List.of(HANDLE));
    assertThat(result).isEmpty();
  }

  @Test
  void testEqualDigitalMediaDifferentOriginalData()
      throws JsonProcessingException, DigitalSpecimenNotFoundException {
    // Given
    given(repository.getDigitalMedia(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(
        List.of(givenDigitalMediaRecordNoOriginalData()));

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaEvent()));

    // Then
    then(repository).should().updateLastChecked(List.of(HANDLE));
    assertThat(result).isEmpty();
  }

  @Test
  void testUnequalDigitalMediaNoHandleUpdate() throws Exception {
    // Given
    var expected = List.of(givenDigitalMediaRecordWithVersion(2));
    given(repository.getDigitalMedia(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(
        List.of(givenDigitalMediaRecord(FORMAT_2)));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalMedia(expected)).willReturn(bulkResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(false);

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaEvent()));

    // Then
    then(handleComponent).shouldHaveNoInteractions();
    then(repository).should().createDigitalMediaRecord(expected);
    then(publisherService).should()
        .publishUpdateEvent(givenDigitalMediaRecordWithVersion(2), givenJsonPatch());
    then(annotationPublisherService).should().publishAnnotationUpdatedMedia(anySet());
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testUnequalDigitalMediaHandleUpdate() throws Exception {
    // Given
    var expected = List.of(givenDigitalMediaRecordWithVersion(2));
    given(repository.getDigitalMedia(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(
        List.of(givenDigitalMediaRecord(FORMAT_2)));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalMedia(expected)).willReturn(bulkResponse);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaEvent()));

    // Then
    then(fdoRecordService).should()
        .buildPatchDeleteRequest(List.of(givenDigitalMediaRecordWithVersion(2)));
    then(handleComponent).should().updateHandle(any());
    then(repository).should().createDigitalMediaRecord(expected);
    then(publisherService).should()
        .publishUpdateEvent(givenDigitalMediaRecordWithVersion(2), givenJsonPatch());
    then(annotationPublisherService).should().publishAnnotationUpdatedMedia(anySet());
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testUnequalDigitalMediaHandleUpdateFailed() throws Exception {
    // Given
    given(repository.getDigitalMedia(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(
        List.of(givenDigitalMediaRecord(FORMAT_2)));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(PidCreationException.class).when(handleComponent).updateHandle(any());

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaEvent()));

    // Then
    then(fdoRecordService).should()
        .buildPatchDeleteRequest(List.of(givenDigitalMediaRecordWithVersion(2)));
    then(repository).shouldHaveNoMoreInteractions();
    then(publisherService).should().deadLetterEvent(givenDlqTransferEventUpdate());
    then(annotationPublisherService).shouldHaveNoInteractions();
    assertThat(result).isEmpty();
  }

  @Test
  void testUnequalDigitalMediaHandleUpdateFailedKafkaFailed() throws Exception {
    // Given
    given(repository.getDigitalMedia(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(
        List.of(givenDigitalMediaRecord(FORMAT_2)));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(PidCreationException.class).when(handleComponent).updateHandle(any());
    doThrow(JsonProcessingException.class).when(publisherService)
        .deadLetterEvent(givenDigitalMediaEvent());

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaEvent()));

    // Then
    then(repository).shouldHaveNoMoreInteractions();
    then(annotationPublisherService).shouldHaveNoInteractions();
    assertThat(result).isEmpty();
  }

  @Test
  void testNewDigitalMedia()
      throws Exception {
    // Given
    var expected = List.of(givenDigitalMediaRecord());
    given(repository.getDigitalMedia(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willReturn(givenPidMap(1));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalMedia(
        Set.of(givenDigitalMediaRecord()))).willReturn(bulkResponse);

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaEvent()));

    // Then
    then(fdoRecordService).should()
        .buildPostHandleRequest(List.of(TestUtils.givenDigitalMediaWrapper()));
    then(handleComponent).should().postHandle(any());
    then(repository).should().createDigitalMediaRecord(Set.of(expected.get(0)));
    then(publisherService).should().publishCreateEvent(expected.get(0));
    then(publisherService).should().publishAnnotationRequestEvent(MAS, expected.get(0));
    then(annotationPublisherService).should().publishAnnotationNewMedia(Set.of(expected.get(0)));
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testDuplicateNewDigitalMedia()
      throws Exception {
    // Given
    var expected = List.of(givenDigitalMediaRecord());
    given(repository.getDigitalMedia(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willReturn(givenPidMap(1));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalMedia(
        Set.of(givenDigitalMediaRecord()))).willReturn(bulkResponse);

    // When
    var result = service.handleMessage(
        List.of(givenDigitalMediaEvent(), givenDigitalMediaEvent()));

    // Then
    then(fdoRecordService).should()
        .buildPostHandleRequest(List.of(TestUtils.givenDigitalMediaWrapper()));
    then(handleComponent).should().postHandle(any());
    then(repository).should().createDigitalMediaRecord(Set.of(expected.get(0)));
    then(publisherService).should().publishCreateEvent(expected.get(0));
    then(publisherService).should().publishAnnotationRequestEvent(MAS, expected.get(0));
    then(publisherService).should()
        .republishDigitalMedia(givenDigitalMediaEvent());
    then(annotationPublisherService).should().publishAnnotationNewMedia(Set.of(expected.get(0)));
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testNewDigitalMediaIOException()
      throws Exception {
    // Given
    given(repository.getDigitalMedia(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willReturn(givenPidMap(1));
    given(elasticRepository.indexDigitalMedia(
        Set.of(givenDigitalMediaRecord()))).willThrow(IOException.class);

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaEvent()));

    // Then
    then(fdoRecordService).should()
        .buildPostHandleRequest(List.of(TestUtils.givenDigitalMediaWrapper()));
    then(fdoRecordService).should()
        .buildRollbackCreationRequest(List.of(givenDigitalMediaRecord()));
    then(handleComponent).should().postHandle(any());
    then(handleComponent).should().rollbackHandleCreation(any());
    then(repository).should().createDigitalMediaRecord(Set.of(givenDigitalMediaRecord()));
    then(repository).should().rollBackDigitalMedia(HANDLE);
    then(publisherService).should().deadLetterEvent(givenDigitalMediaEvent());
    then(annotationPublisherService).shouldHaveNoInteractions();
    assertThat(result).isEmpty();
  }

  @Test
  void testNewDigitalMediaIOExceptionHandleRollbackFailed()
      throws Exception {
    // Given
    given(repository.getDigitalMedia(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willReturn(givenPidMap(1));
    given(elasticRepository.indexDigitalMedia(
        Set.of(givenDigitalMediaRecord()))).willThrow(IOException.class);
    doThrow(PidCreationException.class).when(handleComponent).rollbackHandleCreation(any());

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaEvent()));

    // Then
    then(fdoRecordService).should()
        .buildPostHandleRequest(List.of(TestUtils.givenDigitalMediaWrapper()));
    then(handleComponent).should().postHandle(any());
    then(repository).should().createDigitalMediaRecord(Set.of(givenDigitalMediaRecord()));
    then(repository).should().rollBackDigitalMedia(HANDLE);
    then(publisherService).should().deadLetterEvent(givenDigitalMediaEvent());
    then(annotationPublisherService).shouldHaveNoInteractions();
    assertThat(result).isEmpty();
  }

  @Test
  void testNewDigitalMediaPartialElasticFailed()
      throws Exception {
    // Given
    var secondEvent = givenDigitalMediaEvent(DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2);
    var thirdEvent = givenDigitalMediaEvent(DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3);
    given(repository.getDigitalMedia(anyList(), anyList())).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willReturn(givenPidMap(3));
    givenBulkResponse();
    given(elasticRepository.indexDigitalMedia(anySet())).willReturn(bulkResponse);

    // When
    var result = service.handleMessage(
        List.of(givenDigitalMediaEvent(), secondEvent, thirdEvent));

    // Then
    then(fdoRecordService).should().buildPostHandleRequest(List.of(
        givenDigitalMediaWrapper(DIGITAL_SPECIMEN_ID_2, FORMAT, MEDIA_URL_2,
            TYPE),
        givenDigitalMediaWrapper(DIGITAL_SPECIMEN_ID_3, FORMAT, MEDIA_URL_3,
            TYPE),
        TestUtils.givenDigitalMediaWrapper()
    ));
    then(fdoRecordService).should().buildRollbackCreationRequest(List.of(
        givenDigitalMediaRecordPhysical(HANDLE_2, DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2, TYPE)));
    then(handleComponent).should().rollbackHandleCreation(any());
    then(repository).should().createDigitalMediaRecord(anySet());
    then(repository).should().rollBackDigitalMedia(HANDLE_2);
    then(publisherService).should().deadLetterEvent(secondEvent);
    then(annotationPublisherService).should().publishAnnotationNewMedia(Set.of(
        givenDigitalMediaRecordPhysical(HANDLE_3, DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3, TYPE),
        givenDigitalMediaRecord()));
    assertThat(result).isEqualTo(List.of(
        givenDigitalMediaRecord(),
        givenDigitalMediaRecordPhysical(HANDLE_3, DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3, TYPE)
    ));
  }

  @Test
  void testNewDigitalMediaPartialElasticFailedKafkaDlqFailed()
      throws Exception {
    // Given
    var secondEvent = givenDigitalMediaEvent(DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2);
    var thirdEvent = givenDigitalMediaEvent(DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3);
    var secondRecord =
        givenDigitalMediaRecordPhysical(HANDLE_2, DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2, TYPE);
    var thirdRecord = givenDigitalMediaRecordPhysical(HANDLE_3, DIGITAL_SPECIMEN_ID_3,
        MEDIA_URL_3, TYPE);

    given(repository.getDigitalMedia(anyList(), anyList())).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willReturn(givenPidMap(3));
    givenBulkResponse();
    given(elasticRepository.indexDigitalMedia(anySet())).willReturn(bulkResponse);
    doNothing().doThrow(JsonProcessingException.class).when(publisherService)
        .publishCreateEvent(givenDigitalMediaRecord());
    doThrow(JsonProcessingException.class).when(publisherService).publishCreateEvent(thirdRecord);

    // When
    var result = service.handleMessage(
        List.of(givenDigitalMediaEvent(), secondEvent, thirdEvent));

    // Then
    then(fdoRecordService).should().buildPostHandleRequest(List.of(
        secondRecord.digitalMediaWrapper(),
        thirdRecord.digitalMediaWrapper(),
        TestUtils.givenDigitalMediaWrapper()
    ));
    then(fdoRecordService).should()
        .buildRollbackCreationRequest(List.of(secondRecord, thirdRecord));
    then(handleComponent).should().rollbackHandleCreation(any());
    then(repository).should().createDigitalMediaRecord(anySet());
    then(repository).should().rollBackDigitalMedia(HANDLE_2);
    then(repository).should().rollBackDigitalMedia(HANDLE_3);
    then(elasticRepository).should().rollbackDigitalMedia(thirdRecord);
    then(publisherService).should().deadLetterEvent(secondEvent);
    then(publisherService).should().deadLetterEvent(thirdEvent);
    then(annotationPublisherService).should()
        .publishAnnotationNewMedia(Set.of(givenDigitalMediaRecord()));
    assertThat(result).isEqualTo(List.of(givenDigitalMediaRecord()));
  }

  @Test
  void testNewDigitalMediaPartialElasticFailedHandleRollbackFailed()
      throws Exception {
    // Given
    var secondEvent = givenDigitalMediaEvent(DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2);
    var thirdEvent = givenDigitalMediaEvent(DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3);
    given(repository.getDigitalMedia(anyList(), anyList())).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willReturn(givenPidMap(3));
    givenBulkResponse();
    given(elasticRepository.indexDigitalMedia(anySet())).willReturn(bulkResponse);
    doThrow(PidCreationException.class).when(handleComponent).rollbackHandleCreation(any());

    // When
    var result = service.handleMessage(
        List.of(givenDigitalMediaEvent(), secondEvent, thirdEvent));

    // Then
    then(repository).should().createDigitalMediaRecord(anySet());
    then(repository).should().rollBackDigitalMedia(HANDLE_2);
    then(fdoRecordService).should().buildRollbackCreationRequest(List.of(
        givenDigitalMediaRecordPhysical(HANDLE_2, DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2, TYPE)));
    then(handleComponent).should().rollbackHandleCreation(any());
    then(publisherService).should().deadLetterEvent(secondEvent);
    then(annotationPublisherService).should().publishAnnotationNewMedia(Set.of(
        givenDigitalMediaRecordPhysical(HANDLE_3, DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3, TYPE),
        givenDigitalMediaRecord()
    ));
    assertThat(result).isEqualTo(List.of(
        givenDigitalMediaRecord(),
        givenDigitalMediaRecordPhysical(HANDLE_3, DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3, TYPE)
    ));
  }

  @Test
  void testNewDigitalMediaPartialElasticFailedHandleRollbackFailedKafkaFailed()
      throws Exception {
    // Given
    var secondEvent = givenDigitalMediaEvent(DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2);
    var thirdEvent = givenDigitalMediaEvent(DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3);
    given(repository.getDigitalMedia(anyList(), anyList())).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willReturn(givenPidMap(3));
    givenBulkResponse();
    given(elasticRepository.indexDigitalMedia(anySet())).willReturn(bulkResponse);
    doThrow(PidCreationException.class).when(handleComponent).rollbackHandleCreation(any());
    doThrow(JsonProcessingException.class).when(publisherService).deadLetterEvent(secondEvent);

    // When
    var result = service.handleMessage(
        List.of(givenDigitalMediaEvent(), secondEvent, thirdEvent));

    // Then
    then(repository).should().createDigitalMediaRecord(anySet());
    then(repository).should().rollBackDigitalMedia(HANDLE_2);
    then(annotationPublisherService).should().publishAnnotationNewMedia(Set.of(
        givenDigitalMediaRecordPhysical(HANDLE_3, DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3, TYPE),
        givenDigitalMediaRecord()
    ));
    then(fdoRecordService).should().buildRollbackCreationRequest(List.of(
        givenDigitalMediaRecordPhysical(HANDLE_2, DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2, TYPE)));
    assertThat(result).isEqualTo(
        List.of(
            givenDigitalMediaRecord(),
            givenDigitalMediaRecordPhysical(HANDLE_3, DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3, TYPE)
        ));
  }

  @Test
  void testNewDigitalMediaKafkaFailed()
      throws Exception {
    // Given
    var expected = List.of(givenDigitalMediaRecord());
    given(repository.getDigitalMedia(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willReturn(givenPidMap(1));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalMedia(
        Set.of(givenDigitalMediaRecord()))).willReturn(bulkResponse);
    doThrow(JsonProcessingException.class).when(publisherService).publishCreateEvent(any(
        DigitalMediaRecord.class));

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaEvent()));

    // Then
    then(fdoRecordService).should()
        .buildPostHandleRequest(List.of(TestUtils.givenDigitalMediaWrapper()));
    then(fdoRecordService).should()
        .buildRollbackCreationRequest(List.of(givenDigitalMediaRecord()));
    then(handleComponent).should().rollbackHandleCreation(any());
    then(repository).should().createDigitalMediaRecord(anySet());
    then(repository).should().rollBackDigitalMedia(HANDLE);
    then(elasticRepository).should().rollbackDigitalMedia(expected.get(0));
    then(publisherService).should().deadLetterEvent(givenDigitalMediaEvent());
    then(annotationPublisherService).should().publishAnnotationNewMedia(Set.of());
    assertThat(result).isEmpty();
  }

  @Test
  void testUpdateDigitalMediaHandlesDoNotNeedUpdate() throws Exception {
    // Given
    var secondEvent = givenDigitalMediaEvent(DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2);
    var thirdEvent = givenDigitalMediaEvent(DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3);
    given(repository.getDigitalMedia(anyList(), anyList())).willReturn(List.of(
        givenDigitalMediaRecordPhysical(HANDLE, DIGITAL_SPECIMEN_ID, MEDIA_URL_1,
            "Another Type"),
        givenDigitalMediaRecordPhysical(HANDLE_2, DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2,
            "Another Type"),
        givenDigitalMediaRecordPhysical(HANDLE_3, DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3,
            "Another Type")
    ));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(false);
    givenBulkResponse();
    given(elasticRepository.indexDigitalMedia(anyList())).willReturn(bulkResponse);

    // When
    var result = service.handleMessage(
        List.of(givenDigitalMediaEvent(), secondEvent, thirdEvent));

    // Then
    then(fdoRecordService).shouldHaveNoMoreInteractions();
    then(repository).should(times(2)).createDigitalMediaRecord(anyList());
    then(publisherService).should().deadLetterEvent(secondEvent);
    then(annotationPublisherService).should().publishAnnotationUpdatedMedia(anySet());
    assertThat(result).hasSize(2);
  }

  @Test
  void testUpdateDigitalMediaPartialElasticFailed()
      throws Exception {
    // Given
    var secondEvent = givenDigitalMediaEvent(DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2);
    var thirdEvent = givenDigitalMediaEvent(DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3);
    given(repository.getDigitalMedia(anyList(), anyList())).willReturn(List.of(
        givenDigitalMediaRecordPhysical(HANDLE, DIGITAL_SPECIMEN_ID, MEDIA_URL_1,
            "Another Type"),
        givenDigitalMediaRecordPhysical(HANDLE_2, DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2,
            "Another Type"),
        givenDigitalMediaRecordPhysical(HANDLE_3, DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3,
            "Another Type")
    ));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    givenBulkResponse();
    given(elasticRepository.indexDigitalMedia(anyList())).willReturn(bulkResponse);

    // When
    var result = service.handleMessage(
        List.of(givenDigitalMediaEvent(), secondEvent, thirdEvent));

    // Then
    then(handleComponent).should().updateHandle(any());
    then(fdoRecordService).should().buildPatchDeleteRequest(List.of(
        givenDigitalMediaRecordPhysical(HANDLE_2, DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2,
            "Another Type")));
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should(times(2)).createDigitalMediaRecord(anyList());
    then(publisherService).should().deadLetterEvent(secondEvent);
    then(annotationPublisherService).should().publishAnnotationUpdatedMedia(anySet());
    assertThat(result).hasSize(2);
  }

  @Test
  void testUpdateDigitalMediaPartialElasticFailedFailedKafkaDlqFailed()
      throws Exception {
    // Given
    var secondEvent = givenDigitalMediaEvent(DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2);
    var thirdEvent = givenDigitalMediaEvent(DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3);
    var secondRecord =
        givenDigitalMediaRecordPhysical(HANDLE_2, DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2,
            "Another Type");
    var thirdRecord = givenDigitalMediaRecordPhysical(HANDLE_3, DIGITAL_SPECIMEN_ID_3,
        MEDIA_URL_3, "Another Type");

    given(repository.getDigitalMedia(anyList(), anyList())).willReturn(List.of(
        givenDigitalMediaRecordPhysical(HANDLE, DIGITAL_SPECIMEN_ID, MEDIA_URL_1,
            "Another Type"),
        secondRecord,
        thirdRecord
    ));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    givenBulkResponse();
    given(elasticRepository.indexDigitalMedia(anyList())).willReturn(bulkResponse);
    doNothing().doThrow(JsonProcessingException.class).when(publisherService)
        .publishUpdateEvent(any(), any());

    // When
    var result = service.handleMessage(
        List.of(givenDigitalMediaEvent(), secondEvent, thirdEvent));

    // Then
    then(handleComponent).should().updateHandle(any());
    then(fdoRecordService).should().buildPatchDeleteRequest(List.of(secondRecord, thirdRecord));
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should(times(3)).createDigitalMediaRecord(anyList());
    then(publisherService).should().deadLetterEvent(secondEvent);
    then(publisherService).should().deadLetterEvent(thirdEvent);
    then(elasticRepository).should().rollbackVersion(thirdRecord);
    then(annotationPublisherService).should().publishAnnotationUpdatedMedia(anySet());
    assertThat(result).isEqualTo(List.of(givenDigitalMediaRecordWithVersion(2)));
  }

  @Test
  void testUpdateDigitalMediaPartialElasticFailedHandleRollbackFailed() throws Exception {
    // Given
    var secondEvent = givenDigitalMediaEvent(DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2);
    var thirdEvent = givenDigitalMediaEvent(DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3);
    given(repository.getDigitalMedia(anyList(), anyList())).willReturn(List.of(
        givenDigitalMediaRecordPhysical(HANDLE, DIGITAL_SPECIMEN_ID, MEDIA_URL_1,
            "Another Type"),
        givenDigitalMediaRecordPhysical(HANDLE_2, DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2,
            "Another Type"),
        givenDigitalMediaRecordPhysical(HANDLE_3, DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3,
            "Another Type")
    ));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    givenBulkResponse();
    given(elasticRepository.indexDigitalMedia(anyList())).willReturn(bulkResponse);
    doThrow(PidCreationException.class).when(handleComponent).rollbackHandleUpdate(any());

    // When
    var result = service.handleMessage(
        List.of(givenDigitalMediaEvent(), secondEvent, thirdEvent));

    // Then
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should(times(2)).createDigitalMediaRecord(anyList());
    then(publisherService).should().deadLetterEvent(secondEvent);
    then(annotationPublisherService).should().publishAnnotationUpdatedMedia(anySet());
    assertThat(result).hasSize(2);
  }

  @Test
  void testUpdateDigitalMediaKafkaFailed() throws Exception {
    // Given
    var expected = List.of(givenDigitalMediaRecordWithVersion(2));
    given(repository.getDigitalMedia(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(
        List.of(givenDigitalMediaRecord(FORMAT_2)));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalMedia(expected)).willReturn(bulkResponse);
    doThrow(JsonProcessingException.class).when(publisherService)
        .publishUpdateEvent(givenDigitalMediaRecordWithVersion(2), givenJsonPatch());
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaEvent()));

    // Then
    then(fdoRecordService).should()
        .buildPatchDeleteRequest(List.of(givenDigitalMediaRecord(FORMAT_2)));
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(repository).should(times(2)).createDigitalMediaRecord(anyList());
    then(elasticRepository).should().rollbackVersion(givenDigitalMediaRecord(FORMAT_2));
    then(publisherService).should().deadLetterEvent(givenDigitalMediaEvent());
    then(annotationPublisherService).should().publishAnnotationUpdatedMedia(Set.of());
    assertThat(result).isEmpty();
  }

  @Test
  void testUpdateDigitalMediaIOException() throws Exception {
    // Given
    var expected = List.of(givenDigitalMediaRecordWithVersion(2));
    given(repository.getDigitalMedia(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(
        List.of(givenDigitalMediaRecord(FORMAT_2)));
    given(elasticRepository.indexDigitalMedia(expected)).willThrow(IOException.class);
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaEvent()));

    // Then
    then(fdoRecordService).should().buildPatchDeleteRequest(List.of(
        givenDigitalMediaRecord(FORMAT_2)));
    then(repository).should(times(2)).createDigitalMediaRecord(anyList());
    then(publisherService).should().deadLetterEvent(givenDigitalMediaEvent());
    then(annotationPublisherService).shouldHaveNoInteractions();
    assertThat(result).isEmpty();
  }

  @Test
  void testNewDigitalMediaHandleException()
      throws Exception {
    // Given
    given(repository.getDigitalMedia(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willThrow(PidCreationException.class);

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaEvent()));

    // Then
    then(repository).shouldHaveNoMoreInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
    then(publisherService).should().deadLetterEvent(givenDigitalMediaEvent());
    then(annotationPublisherService).shouldHaveNoInteractions();
    assertThat(result).isEmpty();
  }

  @Test
  void testNewDigitalMediaHandleExceptionKafkaFailed() throws Exception {
    // Given
    given(repository.getDigitalMedia(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willThrow(PidCreationException.class);
    doThrow(JsonProcessingException.class).when(publisherService)
        .deadLetterEvent(givenDigitalMediaEvent());

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaEvent()));

    // Then
    then(repository).shouldHaveNoMoreInteractions();
    then(elasticRepository).shouldHaveNoInteractions();
    then(annotationPublisherService).shouldHaveNoInteractions();
    assertThat(result).isEmpty();
  }

  @Test
  void testDigitalMediaMissingKafka()
      throws JsonProcessingException, DigitalSpecimenNotFoundException {
    // Given

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaEvent()));

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void testDigitalSpecimenMissingDoi() {
    // Given
    given(digitalSpecimenRepository.getExistingSpecimen(Set.of(DIGITAL_SPECIMEN_ID))).willReturn(
        Set.of());
    given(environment.matchesProfiles(Profiles.WEB)).willReturn(true);

    // When / Then
    assertThrows(DigitalSpecimenNotFoundException.class,
        () -> service.handleMessage(List.of(givenDigitalMediaEvent())));
  }

  @Test
  void testDigitalSpecimenExistsWebProfile() throws Exception {
    // Given
    var existingIds = Set.of(DIGITAL_SPECIMEN_ID, DIGITAL_SPECIMEN_ID_2, DIGITAL_SPECIMEN_ID_3);
    var expected = List.of(givenDigitalMediaRecord(),
        givenDigitalMediaRecord(HANDLE_2, DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2, FORMAT),
        givenDigitalMediaRecord(HANDLE_3, DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3,
            FORMAT));
    var messages = List.of(givenDigitalMediaEvent(),
        givenDigitalMediaEvent(DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2),
        givenDigitalMediaEvent(DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3));
    given(digitalSpecimenRepository.getExistingSpecimen(existingIds)).willReturn(existingIds);
    given(environment.matchesProfiles(Profiles.WEB)).willReturn(true);
    given(handleComponent.postHandle(anyList())).willReturn(givenPidMap(3));
    given(bulkResponse.errors()).willReturn(false);
    given(elasticRepository.indexDigitalMedia(Set.copyOf(expected))).willReturn(bulkResponse);

    // When
    var result = service.handleMessage(messages);

    // Then
    then(fdoRecordService).should().buildPostHandleRequest(anyList());
    then(handleComponent).should().postHandle(any());
    then(repository).should().createDigitalMediaRecord(Set.copyOf(expected));
    then(publisherService).should(times(3)).publishCreateEvent(any(DigitalMediaRecord.class));
    then(publisherService).should(times(3))
        .publishAnnotationRequestEvent(eq(MAS), any(DigitalMediaRecord.class));
    assertThat(result).hasSameElementsAs(expected);
  }

  @Test
  void testCreateMediaDataAccessException() throws Exception {
    given(repository.getDigitalMedia(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(List.of());
    given(handleComponent.postHandle(anyList())).willReturn(givenPidMap(1));
    doThrow(DataAccessException.class).when(repository).createDigitalMediaRecord(any());

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaEvent()));

    // Then
    assertThat(result).isEmpty();
    then(handleComponent).should().rollbackHandleCreation(any());
    then(annotationPublisherService).shouldHaveNoInteractions();
    then(publisherService).should().deadLetterEvent(any());
  }

  @Test
  void testUpdateMediaDataAccessException() throws Exception {
    given(repository.getDigitalMedia(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1))).willReturn(
        List.of(givenDigitalMediaRecord(FORMAT_2)));
    given(fdoRecordService.handleNeedsUpdate(any(), any())).willReturn(true);
    doThrow(DataAccessException.class).when(repository).createDigitalMediaRecord(any());

    // When
    var result = service.handleMessage(List.of(givenDigitalMediaEvent()));

    // Then
    assertThat(result).isEmpty();
    then(handleComponent).should().rollbackHandleUpdate(any());
    then(annotationPublisherService).shouldHaveNoInteractions();
    then(publisherService).should().deadLetterEvent(any());
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

  private DigitalMediaEvent givenDlqTransferEventUpdate() throws Exception {
    var mediaRecord = new UpdatedDigitalMediaRecord(
        givenDigitalMediaRecord(),
        List.of(MAS),
        givenDigitalMediaRecord(FORMAT_2),
        givenJsonPatch()
    );
    return new DigitalMediaEvent(mediaRecord.automatedAnnotations(),
        new DigitalMediaWrapper(
            mediaRecord.digitalMediaRecord().digitalMediaWrapper().type(),
            mediaRecord.digitalMediaRecord().digitalMediaWrapper().digitalSpecimenID(),
            mediaRecord.digitalMediaRecord().digitalMediaWrapper().attributes(),
            mediaRecord.digitalMediaRecord().digitalMediaWrapper().originalAttributes()
        ));
  }

}
