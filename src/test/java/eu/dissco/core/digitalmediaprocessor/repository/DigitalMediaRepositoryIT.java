package eu.dissco.core.digitalmediaprocessor.repository;

import static eu.dissco.core.digitalmediaprocessor.TestUtils.DIGITAL_SPECIMEN_ID;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.DIGITAL_SPECIMEN_ID_2;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.DIGITAL_SPECIMEN_ID_3;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.HANDLE;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.HANDLE_2;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.HANDLE_3;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.MAPPER;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.MEDIA_URL_1;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.MEDIA_URL_2;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.MEDIA_URL_3;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.UPDATED_TIMESTAMP;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenDigitalMediaRecordNoOriginalData;
import static eu.dissco.core.digitalmediaprocessor.database.jooq.Tables.DIGITAL_MEDIA_OBJECT;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import org.jooq.Record1;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DigitalMediaRepositoryIT extends BaseRepositoryIT {

  private DigitalMediaRepository repository;

  @BeforeEach
  void setup() {
    repository = new DigitalMediaRepository(context, MAPPER);
  }


  @Test
  void testCreateDigitalMediaRecord() throws JsonProcessingException {
    // Given
    var digitalMedia = givenDigitalMediaRecord();

    // When
    var result = repository.createDigitalMediaRecord(List.of(digitalMedia));

    // Then
    var actual = repository.getDigitalMedia(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1));
    assertThat(result).hasSize(1);
    assertThat(actual).contains(digitalMedia);
  }


  @Test
  void testUpdateDigitalMediaRecord() throws JsonProcessingException {
    // Given
    var digitalMedia = givenDigitalMediaRecord();
    repository.createDigitalMediaRecord(List.of(digitalMedia));
    var updatedMedia = givenDigitalMediaRecord(HANDLE, "new_format");

    // When
    repository.createDigitalMediaRecord(List.of(updatedMedia));

    // Then
    var actual = repository.getDigitalMedia(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1));
    assertThat(actual).contains(updatedMedia);
  }

  @Test
  void testUpdateMediaOriginalDataChanged() throws JsonProcessingException {
    // Given
    var digitalMedia = givenDigitalMediaRecordNoOriginalData();
    repository.createDigitalMediaRecord(List.of(digitalMedia));
    var originalData = context.select(DIGITAL_MEDIA_OBJECT.ORIGINAL_DATA)
        .from(DIGITAL_MEDIA_OBJECT)
        .where(DIGITAL_MEDIA_OBJECT.ID.eq(HANDLE)).fetchOne(Record1::value1);

    // When
    repository.createDigitalMediaRecord(List.of(givenDigitalMediaRecord()));

    // Then
    var result = context.select(DIGITAL_MEDIA_OBJECT.ORIGINAL_DATA)
        .from(DIGITAL_MEDIA_OBJECT)
        .where(DIGITAL_MEDIA_OBJECT.ID.eq(HANDLE)).fetchOne(Record1::value1);
    assertThat(result).isEqualTo(originalData);
  }

  @Test
  void testUpdateLastChecked() throws JsonProcessingException {
    // Given
    var digitalMedia = givenDigitalMediaRecord();
    repository.createDigitalMediaRecord(List.of(
        digitalMedia,
        givenDigitalMediaRecord(HANDLE_2, DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2),
        givenDigitalMediaRecord(HANDLE_3, DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3)));

    // When
    repository.updateLastChecked(List.of(HANDLE));

    // Then
    var result = context.select(DIGITAL_MEDIA_OBJECT.LAST_CHECKED)
        .from(DIGITAL_MEDIA_OBJECT)
        .where(DIGITAL_MEDIA_OBJECT.ID.eq(HANDLE)).fetchOne(Record1::value1);
    assertThat(result).isAfter(UPDATED_TIMESTAMP);
  }

  @Test
  void testRollbackSpecimen() throws JsonProcessingException {
    // Given
    repository.createDigitalMediaRecord(List.of(
        givenDigitalMediaRecord(HANDLE, DIGITAL_SPECIMEN_ID, MEDIA_URL_1),
        givenDigitalMediaRecord(HANDLE_2, DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2),
        givenDigitalMediaRecord(HANDLE_3, DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3)));

    // When
    repository.rollBackDigitalMedia(HANDLE_2);

    // Then
    var result = repository.getDigitalMedia(List.of(DIGITAL_SPECIMEN_ID_2),
        List.of(MEDIA_URL_2));
    assertThat(result).isEmpty();
  }


}
