package eu.dissco.core.digitalmediaobjectprocessor.service;

import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MAPPER;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenMediaEvent;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.DigitalSpecimenNotFoundException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;


@ExtendWith(MockitoExtension.class)
class KafkaConsumerServiceTest {

  @Mock
  private ProcessingService processingService;
  @Mock
  private KafkaPublisherService publisherService;

  private KafkaConsumerService service;

  @BeforeEach
  void setup() {
    service = new KafkaConsumerService(MAPPER, processingService, publisherService);
  }

  @Test
  void testGetMessages()
      throws DigitalSpecimenNotFoundException, JsonProcessingException {
    // Given
    var message = givenMessage();

    // When
    service.getMessages(List.of(message));

    // Then
    then(processingService).should().handleMessage(List.of(givenMediaEvent()), false);
    then(publisherService).shouldHaveNoInteractions();
  }

  @Test
  void testGetInvalidMessages() {
    // Given
    var message = givenInvalidMessage();

    // When
    service.getMessages(List.of(message));

    // Then
    then(processingService).shouldHaveNoInteractions();
    then(publisherService).should().deadLetterRaw(message);
  }

  @Test
  void testProcessingThrowsException()
      throws JsonProcessingException, DigitalSpecimenNotFoundException {
    // Given
    var message = givenMessage();
    given(processingService.handleMessage(List.of(givenMediaEvent()), false)).willThrow(
        new DigitalSpecimenNotFoundException("Not found"));

    // When
    service.getMessages(List.of(message));

    // Then
    then(publisherService).shouldHaveNoInteractions();
    then(processingService).shouldHaveNoMoreInteractions();
  }

  private String givenInvalidMessage() {
    return """
        {
          "enrichmentList": ["OCR"],
          "digitalMediaasdbject": {
            "dcterms:type": "2DImageObject",
            "ods:physicalSpecimenId": "045db6cb-5f06-4c19-b0f6-9620bdff3ae4:040ck2b86",
            "ods:attributes": {
              "ac:accessURI": "http://data.rbge.org.uk/living/19942272",
              "ods:sourceSystemId": "20.5000.1025/WDP-JYE-73C",
              "dcterms:format": "image/jpeg",
              "dcterms:license": "http://creativecommons.org/licenses/by-nc/3.0/"
            },
            "ods:originalAttributes": {
              "dwca:ID": "045db6cb-5f06-4c19-b0f6-9620bdff3ae4",
               "dcterms:type": "StillImage",
               "dcterms:title": "19942272",
               "dcterms:format": "image/jpeg",
               "dcterms:creator": "Unknown",
               "dcterms:license": "http://creativecommons.org/licenses/by-nc/3.0/",
               "dcterms:publisher": "Royal Botanic Garden Edinburgh",
               "dcterms:identifier": "http://repo.rbge.org.uk/image_server.php?kind=1500&path_base64=L2l0ZW1faW1hZ2VzL2FjY2Vzc2lvbnMvMTkvOTQvMjIvNzIvUGhvdG9fNTFjMWNlNzM5ZDE2Zi5qcGc=",
               "dcterms:references": "http://data.rbge.org.uk/living/19942272",
               "dcterms:description": "Image of living collection specimen with accession number 19942272 by Unknown",
               "dcterms:rightsHolder": "Copyright Royal Botanic Garden Edinburgh. Contact us for rights to commercial use."
            }
          }
        }""";
  }

  private String givenMessage() {
    return """
        {
          "enrichmentList": ["OCR"],
          "digitalMediaObject": {
            "dcterms:type": "2DImageObject",
            "ods:physicalSpecimenId": "045db6cb-5f06-4c19-b0f6-9620bdff3ae4:040ck2b86",
            "ods:attributes": {
              "ac:accessURI": "http://data.rbge.org.uk/living/19942272",
              "ods:sourceSystemId": "20.5000.1025/WDP-JYE-73C",
              "dcterms:format": "image/jpeg",
              "dcterms:license": "http://creativecommons.org/licenses/by-nc/3.0/"
            },
            "ods:originalAttributes": {
              "dwca:ID": "045db6cb-5f06-4c19-b0f6-9620bdff3ae4",
               "dcterms:type": "StillImage",
               "dcterms:title": "19942272",
               "dcterms:format": "image/jpeg",
               "dcterms:creator": "Unknown",
               "dcterms:license": "http://creativecommons.org/licenses/by-nc/3.0/",
               "dcterms:publisher": "Royal Botanic Garden Edinburgh",
               "dcterms:identifier": "http://repo.rbge.org.uk/image_server.php?kind=1500&path_base64=L2l0ZW1faW1hZ2VzL2FjY2Vzc2lvbnMvMTkvOTQvMjIvNzIvUGhvdG9fNTFjMWNlNzM5ZDE2Zi5qcGc=",
               "dcterms:references": "http://data.rbge.org.uk/living/19942272",
               "dcterms:description": "Image of living collection specimen with accession number 19942272 by Unknown",
               "dcterms:rightsHolder": "Copyright Royal Botanic Garden Edinburgh. Contact us for rights to commercial use."
            }
          }
        }""";
  }


}