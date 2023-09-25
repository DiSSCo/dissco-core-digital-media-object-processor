package eu.dissco.core.digitalmediaobjectprocessor.repository;

import static eu.dissco.core.digitalmediaobjectprocessor.database.jooq.Tables.DIGITAL_SPECIMEN;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DigitalSpecimenRepositoryIT extends BaseRepositoryIT {

  private DigitalSpecimenRepository repository;

  @BeforeEach
  void setup() {
    repository = new DigitalSpecimenRepository(context);
  }

  @Test
  void testGetSpecimenId() {
    // Given
    givenSpecimenInserted();

    // When
    var result = repository.getExistingSpecimen(
        List.of("20.5000.1025/460-A7R-QM0", "20.5000.1025/460-A7R-QM1", "20.5000.1025/460-A7R-QM2",
            "20.5000.1025/460-A7R-QM3", "20.5000.1025/460-A7R-XXX"));

    // Then
    assertThat(result).isEqualTo(
        List.of("20.5000.1025/460-A7R-QM0", "20.5000.1025/460-A7R-QM1", "20.5000.1025/460-A7R-QM2",
            "20.5000.1025/460-A7R-QM3"));
  }

  private void givenSpecimenInserted() {
    for (int i = 0; i < 10; i++) {

      context.insertInto(DIGITAL_SPECIMEN)
          .set(DIGITAL_SPECIMEN.ID, "20.5000.1025/460-A7R-QM" + i)
          .set(DIGITAL_SPECIMEN.VERSION, 1)
          .set(DIGITAL_SPECIMEN.TYPE, "BotanySpecimen")
          .set(DIGITAL_SPECIMEN.MIDSLEVEL, (short) 1)
          .set(DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID, "ASJIDJISA:" + i)
          .set(DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_TYPE, "combined")
          .set(DIGITAL_SPECIMEN.ORGANIZATION_ID, "123124")
          .set(DIGITAL_SPECIMEN.SOURCE_SYSTEM_ID, "ssid")
          .set(DIGITAL_SPECIMEN.CREATED, Instant.now())
          .set(DIGITAL_SPECIMEN.LAST_CHECKED, Instant.now())
          .execute();
    }
  }

}
