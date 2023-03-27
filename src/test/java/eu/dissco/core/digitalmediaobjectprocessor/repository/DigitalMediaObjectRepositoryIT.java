package eu.dissco.core.digitalmediaobjectprocessor.repository;

import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.DIGITAL_SPECIMEN_ID;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MAPPER;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MEDIA_URL;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenDigitalMediaObjectRecord;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DigitalMediaObjectRepositoryIT extends BaseRepositoryIT {

  private DigitalMediaObjectRepository repository;

  @BeforeEach
  void setup() {
    repository = new DigitalMediaObjectRepository(context, MAPPER);
  }


  @Test
  void createDigitalMediaObjectRecord() throws JsonProcessingException {
    // Given
    var digitalMedia = givenDigitalMediaObjectRecord();

    // When
    var result = repository.createDigitalMediaObjectRecord(digitalMedia);

    // Then
    var actual = repository.getDigitalMediaObject(DIGITAL_SPECIMEN_ID, MEDIA_URL);
    assertThat(result).isEqualTo(1);
    assertThat(actual).contains(digitalMedia);
  }

  @Test
  void updateDigitalMediaObjectRecord() throws JsonProcessingException {
    // Given
    var digitalMedia = givenDigitalMediaObjectRecord();
    repository.createDigitalMediaObjectRecord(digitalMedia);
    var updatedMedia = givenDigitalMediaObjectRecord("new_format");

    // When
    var result = repository.createDigitalMediaObjectRecord(updatedMedia);

    // Then
    var actual = repository.getDigitalMediaObject(DIGITAL_SPECIMEN_ID, MEDIA_URL);
    assertThat(result).isEqualTo(1);
    assertThat(actual).contains(updatedMedia);
  }

  @Test
  void updateGetDigitalMediaRecord() throws JsonProcessingException {
    // Given
    var digitalMedia = givenDigitalMediaObjectRecord();
    repository.createDigitalMediaObjectRecord(digitalMedia);

    // When
    var actual = repository.getDigitalMediaObject(DIGITAL_SPECIMEN_ID, MEDIA_URL);

    // Then
    assertThat(actual).contains(digitalMedia);
  }


}
