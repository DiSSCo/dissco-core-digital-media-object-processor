package eu.dissco.core.digitalmediaobjectprocessor.repository;

import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalmediaobjectprocessor.database.jooq.Tables.NEW_DIGITAL_SPECIMEN;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;
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
    var result = repository.getSpecimenId(
        List.of("045db6cb-5f06-4c19-b0f6-9620bdff3ae4:040ck2b864"));

    // Then
    assertThat(result).isEqualTo(
        Map.of("045db6cb-5f06-4c19-b0f6-9620bdff3ae4:040ck2b864", "20.5000.1025/460-A7R-QM4"));
  }

  private void givenSpecimenInserted() {
    for (int i = 0; i < 10; i++) {

      context.insertInto(NEW_DIGITAL_SPECIMEN)
          .set(NEW_DIGITAL_SPECIMEN.ID, "20.5000.1025/460-A7R-QM" + i)
          .set(NEW_DIGITAL_SPECIMEN.VERSION, 1)
          .set(NEW_DIGITAL_SPECIMEN.TYPE, "BotanySpecimen")
          .set(NEW_DIGITAL_SPECIMEN.MIDSLEVEL, (short) 1)
          .set(NEW_DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID, PHYSICAL_SPECIMEN_ID + i)
          .set(NEW_DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_TYPE, "combind")
          .set(NEW_DIGITAL_SPECIMEN.ORGANIZATION_ID, "123124")
          .set(NEW_DIGITAL_SPECIMEN.SOURCE_SYSTEM_ID, "ssid")
          .set(NEW_DIGITAL_SPECIMEN.CREATED, Instant.now())
          .set(NEW_DIGITAL_SPECIMEN.LAST_CHECKED, Instant.now())
          .execute();
    }
  }

}
