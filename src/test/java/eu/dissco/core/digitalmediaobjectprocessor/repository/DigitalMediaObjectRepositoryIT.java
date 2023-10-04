package eu.dissco.core.digitalmediaobjectprocessor.repository;

import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.DIGITAL_SPECIMEN_ID;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.DIGITAL_SPECIMEN_ID_2;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.DIGITAL_SPECIMEN_ID_3;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.HANDLE;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.HANDLE_2;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.HANDLE_3;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MAPPER;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MEDIA_URL_1;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MEDIA_URL_2;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MEDIA_URL_3;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.UPDATED_TIMESTAMP;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenDigitalMediaObjectRecord;
import static eu.dissco.core.digitalmediaobjectprocessor.database.jooq.Tables.DIGITAL_MEDIA_OBJECT;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalmediaobjectprocessor.TestUtils;
import java.util.List;
import org.jooq.Record1;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DigitalMediaObjectRepositoryIT extends BaseRepositoryIT {

  private DigitalMediaObjectRepository repository;

  @BeforeEach
  void setup() {
    repository = new DigitalMediaObjectRepository(context, MAPPER);
  }


  @Test
  void testCreateDigitalMediaObjectRecord() throws JsonProcessingException {
    // Given
    var digitalMedia = givenDigitalMediaObjectRecord();

    // When
    var result = repository.createDigitalMediaRecord(List.of(digitalMedia));

    // Then
    var actual = repository.getDigitalMediaObject(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1));
    assertThat(result).hasSize(1);
    assertThat(actual).contains(digitalMedia);
  }


  @Test
  void testUpdateDigitalMediaObjectRecord() throws JsonProcessingException {
    // Given
    var digitalMedia = givenDigitalMediaObjectRecord();
    repository.createDigitalMediaRecord(List.of(digitalMedia));
    var updatedMedia = givenDigitalMediaObjectRecord(HANDLE, "new_format");

    // When
    repository.createDigitalMediaRecord(List.of(updatedMedia));

    // Then
    var actual = repository.getDigitalMediaObject(List.of(DIGITAL_SPECIMEN_ID),
        List.of(MEDIA_URL_1));
    assertThat(actual).contains(updatedMedia);
  }

  @Test
  void testUpdateLastChecked() throws JsonProcessingException {
    // Given
    var digitalMedia = givenDigitalMediaObjectRecord();
    repository.createDigitalMediaRecord(List.of(
        digitalMedia,
        TestUtils.givenDigitalMediaObjectRecord(HANDLE_2, DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2),
        TestUtils.givenDigitalMediaObjectRecord(HANDLE_3, DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3)));

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
        TestUtils.givenDigitalMediaObjectRecord(HANDLE, DIGITAL_SPECIMEN_ID, MEDIA_URL_1),
        TestUtils.givenDigitalMediaObjectRecord(HANDLE_2, DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2),
        TestUtils.givenDigitalMediaObjectRecord(HANDLE_3, DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3)));

    // When
    repository.rollBackDigitalMedia(HANDLE_2);

    // Then
    var result = repository.getDigitalMediaObject(List.of(DIGITAL_SPECIMEN_ID_2),
        List.of(MEDIA_URL_2));
    assertThat(result).isEmpty();
  }


}
