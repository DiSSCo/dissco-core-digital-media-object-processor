package eu.dissco.core.digitalmediaprocessor.service;

import static eu.dissco.core.digitalmediaprocessor.TestUtils.HANDLE;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.MAPPER;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.MAS;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenDigitalMediaUpdatePidEvent;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenMediaEvent;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.then;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalmediaprocessor.TestUtils;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

@ExtendWith(MockitoExtension.class)
class KafkaPublisherServiceTest {

  @Mock
  private KafkaTemplate<String, String> kafkaTemplate;

  @Mock
  private ProvenanceService provenanceService;

  private KafkaPublisherService service;

  @BeforeEach
  void setup() {
    service = new KafkaPublisherService(MAPPER, kafkaTemplate, provenanceService);
  }

  @Test
  void testPublishCreateEvent() throws JsonProcessingException {
    // Given

    // When
    service.publishCreateEvent(givenDigitalMediaRecord());

    // Then
    then(kafkaTemplate).should().send(eq("createUpdateDeleteTopic"), anyString());
  }

  @Test
  void testPublishAnnotationRequestEvent() throws JsonProcessingException {
    // Given
    var digitalMediaRecord = givenDigitalMediaRecord();

    // When
    service.publishAnnotationRequestEvent(MAS, digitalMediaRecord);

    // Then
    then(kafkaTemplate).should().send(eq(MAS), anyString());
  }

  @Test
  void testPublishUpdateEvent() throws JsonProcessingException {
    // Given

    // When
    service.publishUpdateEvent(givenDigitalMediaRecord(),
        givenDigitalMediaRecord(HANDLE, "image/tiff"));

    // Then
    then(kafkaTemplate).should().send(eq("createUpdateDeleteTopic"), anyString());
  }

  @Test
  void testRepublishEvent() throws JsonProcessingException {
    // Given

    // When
    service.republishDigitalMedia(givenMediaEvent());

    // Then
    then(kafkaTemplate).should()
        .send("digital-media-object", MAPPER.writeValueAsString(givenMediaEvent()));
  }

  @Test
  void testDeadLetterRaw() {
    // Given
    var messageString = "Given String Event";

    // When
    service.deadLetterRaw(messageString);

    // Then
    then(kafkaTemplate).should()
        .send("digital-media-object-dlq", messageString);
  }

  @Test
  void testDeadLetterEvent() throws JsonProcessingException {
    // Given
    var event = TestUtils.givenDigitalMediaEvent();

    // When
    service.deadLetterEvent(event);

    // Then
    then(kafkaTemplate).should()
        .send("digital-media-object-dlq", MAPPER.writeValueAsString(event));
  }

  @Test
  void testPublishMediaPid() throws JsonProcessingException {
    // Given
    var event = List.of(givenDigitalMediaUpdatePidEvent());

    // When
    service.publishMediaPid(event);

    // Then
    then(kafkaTemplate).should().send("media-update", MAPPER.writeValueAsString(event));
  }

}
