package eu.dissco.core.digitalmediaprocessor.service;

import static eu.dissco.core.digitalmediaprocessor.TestUtils.APP_HANDLE;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.APP_NAME;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.DOI_PREFIX;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.MAPPER;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.SOURCE_SYSTEM_ID;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.SOURCE_SYSTEM_NAME;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.VERSION;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenJsonPatch;
import static eu.dissco.core.digitalmediaprocessor.domain.AgenRoleType.PROCESSING_SERVICE;
import static eu.dissco.core.digitalmediaprocessor.domain.AgenRoleType.SOURCE_SYSTEM;
import static eu.dissco.core.digitalmediaprocessor.schema.Agent.Type.PROV_SOFTWARE_AGENT;
import static eu.dissco.core.digitalmediaprocessor.utils.AgentUtils.createMachineAgent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalmediaprocessor.TestUtils;
import eu.dissco.core.digitalmediaprocessor.properties.ApplicationProperties;
import eu.dissco.core.digitalmediaprocessor.schema.Agent;
import eu.dissco.core.digitalmediaprocessor.schema.Identifier.DctermsType;
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

  private static List<Agent> givenExpectedProvAgents() {
    return List.of(createMachineAgent(SOURCE_SYSTEM_NAME, SOURCE_SYSTEM_ID, SOURCE_SYSTEM,
        DctermsType.HANDLE, PROV_SOFTWARE_AGENT),
        createMachineAgent(APP_NAME, APP_HANDLE, PROCESSING_SERVICE,
            DctermsType.DOI, PROV_SOFTWARE_AGENT)
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
    assertThat(event.getDctermsIdentifier()).isEqualTo(DOI_PREFIX + TestUtils.HANDLE + "/" + VERSION);
    assertThat(event.getProvActivity().getOdsChangeValue()).isNull();
    assertThat(event.getProvEntity().getProvValue()).isNotNull();
    assertThat(event.getOdsHasAgents()).isEqualTo(givenExpectedProvAgents());
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
    assertThat(event.getDctermsIdentifier()).isEqualTo(DOI_PREFIX + TestUtils.HANDLE + "/" + VERSION);
    assertThat(event.getProvActivity().getOdsChangeValue()).isEqualTo(givenChangeValue());
    assertThat(event.getProvEntity().getProvValue()).isNotNull();
    assertThat(event.getOdsHasAgents()).isEqualTo(givenExpectedProvAgents());
  }

  List<OdsChangeValue> givenChangeValue() {
    return List.of(new OdsChangeValue()
        .withAdditionalProperty("op", "replace")
        .withAdditionalProperty("path", "/dcterms:format")
        .withAdditionalProperty("value", "image/jpeg")
    );
  }
}
