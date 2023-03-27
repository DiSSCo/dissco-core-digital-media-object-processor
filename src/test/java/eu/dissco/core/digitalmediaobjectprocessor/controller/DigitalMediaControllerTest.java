package eu.dissco.core.digitalmediaobjectprocessor.controller;

import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenDigitalMediaObjectRecord;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenMediaEvent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.DigitalSpecimenNotFoundException;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.NoChangesFoundException;
import eu.dissco.core.digitalmediaobjectprocessor.service.ProcessingService;
import javax.xml.transform.TransformerException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

@ExtendWith(MockitoExtension.class)
class DigitalMediaControllerTest {

  @Mock
  private ProcessingService service;
  private DigitalMediaObjectController controller;

  @BeforeEach
  void setup() {
    controller = new DigitalMediaObjectController(service);
  }

  @Test
  void testCreateDigitalMediaObject()
      throws JsonProcessingException, DigitalSpecimenNotFoundException, TransformerException, NoChangesFoundException {
    // Given
    given(service.handleMessage(givenMediaEvent(), true)).willReturn(
        givenDigitalMediaObjectRecord());

    // When
    var result = controller.createDigitalMediaObject(givenMediaEvent());

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    assertThat(result.getBody()).isEqualTo(givenDigitalMediaObjectRecord());
  }

  @Test
  void testNoChanges() {
    // Given

    // When
    var result = controller.handleException(new NoChangesFoundException("No changes"));

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.OK);
  }

  @Test
  void testDSNotFound() {
    // Given

    // When
    var result = controller.handleException(new DigitalSpecimenNotFoundException("No ds"));

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
  }

}
