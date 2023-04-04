package eu.dissco.core.digitalmediaobjectprocessor.service;

import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.AAS;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.HANDLE;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MAPPER;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenDigitalMediaObjectRecord;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenDigitalMediaObjectTransferEvent;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenMediaEvent;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.then;

import com.fasterxml.jackson.core.JsonProcessingException;
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

  private KafkaPublisherService service;

  @BeforeEach
  void setup() {
    service = new KafkaPublisherService(MAPPER, kafkaTemplate);
  }

  @Test
  void testPublishCreateEvent() throws JsonProcessingException {
    // Given

    // When
    service.publishCreateEvent(givenDigitalMediaObjectRecord());

    // Then
    then(kafkaTemplate).should().send(eq("createUpdateDeleteTopic"), anyString());
  }

  @Test
  void testPublishAnnotationRequestEvent() throws JsonProcessingException {
    // Given

    // When
    service.publishAnnotationRequestEvent(AAS, givenDigitalMediaObjectRecord());

    // Then
    then(kafkaTemplate).should().send(eq(AAS), anyString());
  }

  @Test
  void testPublishUpdateEvent() throws JsonProcessingException {
    // Given

    // When
    service.publishUpdateEvent(givenDigitalMediaObjectRecord(),
        givenDigitalMediaObjectRecord(HANDLE, "image/tiff"));

    // Then
    then(kafkaTemplate).should().send(eq("createUpdateDeleteTopic"), anyString());
  }

  @Test
  void testRepublishEvent() throws JsonProcessingException {
    // Given

    // When
    service.republishDigitalMediaObject(givenMediaEvent());

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
    var event = givenDigitalMediaObjectTransferEvent();

    // When
    service.deadLetterEvent(event);

    // Then
    then(kafkaTemplate).should()
        .send("digital-media-object-dlq", MAPPER.writeValueAsString(event));
  }

}
