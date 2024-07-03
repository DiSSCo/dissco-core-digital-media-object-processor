package eu.dissco.core.digitalmediaprocessor.controller;

import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenMediaEvent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import eu.dissco.core.digitalmediaprocessor.TestUtils;
import eu.dissco.core.digitalmediaprocessor.exceptions.DigitalSpecimenNotFoundException;
import eu.dissco.core.digitalmediaprocessor.exceptions.NoChangesFoundException;
import eu.dissco.core.digitalmediaprocessor.service.ProcessingService;
import java.util.List;
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
  private DigitalMediaController controller;

  @BeforeEach
  void setup() {
    controller = new DigitalMediaController(service);
  }

  @Test
  void testCreateDigitalMedia() throws Exception {
    // Given
    given(service.handleMessage(List.of(givenMediaEvent()))).willReturn(
        List.of(TestUtils.givenDigitalMediaRecord()));

    // When
    var result = controller.createDigitalMedia(givenMediaEvent());

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    assertThat(result.getBody()).isEqualTo(TestUtils.givenDigitalMediaRecord());
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
  void testDigitalSpecimenNotFound() {
    // Given

    // When
    var result = controller.handleException(
        new DigitalSpecimenNotFoundException("No digital specimen found"));

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
  }

}
