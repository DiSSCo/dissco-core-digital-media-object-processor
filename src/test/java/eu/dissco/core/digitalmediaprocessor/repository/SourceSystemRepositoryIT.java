package eu.dissco.core.digitalmediaprocessor.repository;

import static eu.dissco.core.digitalmediaprocessor.TestUtils.SOURCE_SYSTEM_NAME;
import static eu.dissco.core.digitalmediaprocessor.database.jooq.tables.SourceSystem.SOURCE_SYSTEM;
import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.core.digitalmediaprocessor.database.jooq.enums.TranslatorType;
import java.time.Instant;
import org.jooq.JSONB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SourceSystemRepositoryIT extends BaseRepositoryIT {

  private SourceSystemRepository repository;

  @BeforeEach
  void setup() {
    this.repository = new SourceSystemRepository(context);
  }

  @AfterEach
  void destroy() {
    context.truncate(SOURCE_SYSTEM).execute();
  }

  @Test
  void testGetNameByID() {
    // Given
    givenInsertSourceSystem();

    // When
    var result = repository.retrieveNameByID("TEST/57Z-6PC-64W");

    // Then
    assertThat(result).isEqualTo(SOURCE_SYSTEM_NAME);
  }

  @Test
  void testGetNameByIDReturnNull() {
    // Given
    givenInsertSourceSystem();

    // When
    var result = repository.retrieveNameByID("https://hdl.handle.net/TEST/XXX-6PC-64W");

    // Then
    assertThat(result).isNull();
  }

  private void givenInsertSourceSystem() {
    context.insertInto(SOURCE_SYSTEM)
        .set(SOURCE_SYSTEM.ID, "TEST/57Z-6PC-64W")
        .set(SOURCE_SYSTEM.NAME, SOURCE_SYSTEM_NAME)
        .set(SOURCE_SYSTEM.ENDPOINT, "http://localhost:8080")
        .set(SOURCE_SYSTEM.DATE_CREATED, Instant.now())
        .set(SOURCE_SYSTEM.DATE_MODIFIED, Instant.now())
        .set(SOURCE_SYSTEM.CREATOR, "test")
        .set(SOURCE_SYSTEM.VERSION, 1)
        .set(SOURCE_SYSTEM.MAPPING_ID, "mapping_id")
        .set(SOURCE_SYSTEM.TRANSLATOR_TYPE, TranslatorType.dwca)
        .set(SOURCE_SYSTEM.DATA, JSONB.valueOf("{}"))
        .execute();
  }

}
