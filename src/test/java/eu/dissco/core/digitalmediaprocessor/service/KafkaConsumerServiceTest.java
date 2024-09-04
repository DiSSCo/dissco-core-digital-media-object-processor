package eu.dissco.core.digitalmediaprocessor.service;

import static eu.dissco.core.digitalmediaprocessor.TestUtils.MAPPER;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenMediaEvent;
import static org.mockito.BDDMockito.then;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalmediaprocessor.exceptions.DigitalSpecimenNotFoundException;
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
  void testGetMessages() throws JsonProcessingException, DigitalSpecimenNotFoundException {
    // Given
    var message = givenMessage();

    // When
    service.getMessages(List.of(message));

    // Then
    then(processingService).should().handleMessage(List.of(givenMediaEvent()));
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

  private String givenInvalidMessage() {
    return """
        {
          "enrichmentList": ["OCR"],
          "digitalMediadbjectWrapper": {
            "ods:type": "Image",
            "ods:digitalSpecimenID": "20.5000.1025/460-A7R-QMJ",
            "ods:attributes": {
              "ac:accessURI": "http://data.rbge.org.uk/living/19942272",
              "ods:sourceSystemId": "20.5000.1025/WDP-JYE-73C",
              "dcterms:format": "image/jpeg",
              "dcterms:license": "http://creativecommons.org/licenses/by-nc/3.0/",
              "ods:organisationID":"https://ror.org/0x123"
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
          "digitalMediaWrapper": {
            "ods:type": "Image",
            "ods:digitalSpecimenID": "20.5000.1025/460-A7R-QMJ",
            "ods:attributes": {
              "ac:accessURI": "http://data.rbge.org.uk/living/19942272",
              "dcterms:format": "image/jpeg",
              "dcterms:license": "http://creativecommons.org/licenses/by-nc/3.0/",
              "ods:organisationID":"https://ror.org/0x123",
              "ods:sourceSystemID": "https://hdl.handle.net/TEST/57Z-6PC-64W",
              "ods:sourceSystemName": "A very nice source system",
              "dcterms:modified": "2022-09-16T08:52:27.391Z"
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
