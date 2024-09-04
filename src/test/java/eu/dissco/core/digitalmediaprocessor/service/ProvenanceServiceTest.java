package eu.dissco.core.digitalmediaprocessor.service;

import static eu.dissco.core.digitalmediaprocessor.TestUtils.APP_HANDLE;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.APP_NAME;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.DOI_PREFIX;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.HANDLE;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.MAPPER;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.SOURCE_SYSTEM_ID;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.SOURCE_SYSTEM_NAME;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.VERSION;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenJsonPatch;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalmediaprocessor.properties.ApplicationProperties;
import eu.dissco.core.digitalmediaprocessor.schema.Agent;
import eu.dissco.core.digitalmediaprocessor.schema.Agent.Type;
import eu.dissco.core.digitalmediaprocessor.schema.OdsChangeValue;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProvenanceServiceTest {

  @Mock
  private ApplicationProperties properties;

  private ProvenanceService service;

  private static List<Agent> givenExpectedAgents() {
    return List.of(
        new Agent()
            .withId(SOURCE_SYSTEM_ID)
            .withType(Type.AS_APPLICATION)
            .withSchemaName(SOURCE_SYSTEM_NAME),
        new Agent()
            .withId(APP_HANDLE)
            .withType(Type.AS_APPLICATION)
            .withSchemaName(APP_NAME)
    );
  }

  @BeforeEach
  void setup() {
    this.service = new ProvenanceService(MAPPER, properties);
  }

  @Test
  void testGenerateCreateEvent() throws JsonProcessingException {
    // Given
    given(properties.getName()).willReturn(APP_NAME);
    given(properties.getPid()).willReturn(APP_HANDLE);
    var digitalSpecimen = givenDigitalMediaRecord();

    // When
    var event = service.generateCreateEvent(digitalSpecimen);

    // Then
    assertThat(event.getOdsID()).isEqualTo(DOI_PREFIX + HANDLE + "/" + VERSION);
    assertThat(event.getProvActivity().getOdsChangeValue()).isNull();
    assertThat(event.getProvEntity().getProvValue()).isNotNull();
    assertThat(event.getOdsHasProvAgent()).isEqualTo(givenExpectedAgents());
  }

  @Test
  void testGenerateUpdateEvent() throws JsonProcessingException {
    // Given
    given(properties.getName()).willReturn(APP_NAME);
    given(properties.getPid()).willReturn(APP_HANDLE);
    var digitalSpecimen = givenDigitalMediaRecord();

    // When
    var event = service.generateUpdateEvent(digitalSpecimen, givenJsonPatch());

    // Then
    assertThat(event.getOdsID()).isEqualTo(DOI_PREFIX + HANDLE + "/" + VERSION);
    assertThat(event.getProvActivity().getOdsChangeValue()).isEqualTo(givenChangeValue());
    assertThat(event.getProvEntity().getProvValue()).isNotNull();
    assertThat(event.getOdsHasProvAgent()).isEqualTo(givenExpectedAgents());
  }

  List<OdsChangeValue> givenChangeValue() {
    return List.of(new OdsChangeValue()
        .withAdditionalProperty("op", "replace")
        .withAdditionalProperty("path", "/dcterms:format")
        .withAdditionalProperty("value", "image/jpeg")
    );
  }
}
