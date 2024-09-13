package eu.dissco.core.digitalmediaprocessor.service;

import static eu.dissco.core.digitalmediaprocessor.TestUtils.APP_HANDLE;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.APP_NAME;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.CREATED;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.DOI_PREFIX;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.HANDLE;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.LICENSE_TESTVAL;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.MAPPER;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.SOURCE_SYSTEM_ID;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.SOURCE_SYSTEM_NAME;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.TYPE;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenAutoAcceptedAnnotation;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenNewAcceptedAnnotation;
import static eu.dissco.core.digitalmediaprocessor.schema.Agent.Type.AS_APPLICATION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mockStatic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalmediaprocessor.domain.UpdatedDigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.properties.ApplicationProperties;
import eu.dissco.core.digitalmediaprocessor.schema.Agent;
import eu.dissco.core.digitalmediaprocessor.schema.AnnotationBody;
import eu.dissco.core.digitalmediaprocessor.schema.AnnotationProcessingRequest;
import eu.dissco.core.digitalmediaprocessor.schema.AnnotationProcessingRequest.OaMotivation;
import eu.dissco.core.digitalmediaprocessor.schema.AnnotationTarget;
import eu.dissco.core.digitalmediaprocessor.schema.OaHasSelector;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AnnotationPublisherServiceTest {

  @Mock
  private KafkaPublisherService kafkaPublisherService;
  @Mock
  private ApplicationProperties applicationProperties;

  private MockedStatic<Instant> mockedInstant;
  private MockedStatic<Clock> mockedClock;
  private AnnotationPublisherService service;

  public static Stream<Arguments> provideUpdateAnnotations() throws JsonProcessingException {
    var classObject = MAPPER.readValue(
        """
            [{
              "op": "add",
              "path": "/ods:hasAssertion/-",
              "value": {
                "@type": "ods:Assertion",
                "dwc:measurementType": "pixelX",
                "dwc:measurementValue": "2048",
                "dwc:measurementUnit": "pixel",
                "ods:AssertionByAgent": {
                  "@id": "https://hdl.handle.net/TEST/123-123-123",
                  "@type": "as:Application",
                  "schema:name": "dissco-nusearch-service",
                  "ods:hasIdentifier": []
                }
              }
            }]
            """, JsonNode.class
    );
    return Stream.of(
        Arguments.of(MAPPER.readTree(
                """
                      [{
                        "op": "replace",
                        "path": "/dcterms:format",
                        "value": "image/jpeg"
                      }]
                    """),
            List.of(givenAcceptedAnnotation(OaMotivation.OA_EDITING,
                new OaHasSelector().withAdditionalProperty("@type", "ods:FieldSelector")
                    .withAdditionalProperty("ods:field",
                        "$['dcterms:format']"),
                new AnnotationBody().withOaValue(List.of("image/jpeg"))
                    .withType("oa:TextualBody").withDctermsReferences(SOURCE_SYSTEM_ID))
            )),
        Arguments.of(classObject,
            List.of(givenAcceptedAnnotation(OaMotivation.ODS_ADDING,
                new OaHasSelector().withAdditionalProperty("@type", "ods:ClassSelector")
                    .withAdditionalProperty("ods:class", "$['ods:hasAssertion']"),
                new AnnotationBody().withOaValue(
                        List.of(MAPPER.writeValueAsString(classObject.get(0).get("value"))))
                    .withType("oa:TextualBody").withDctermsReferences(SOURCE_SYSTEM_ID))
            )),
        Arguments.of(MAPPER.readTree(
                """
                      [{
                        "op": "remove",
                        "path": "/dcterms:description"
                      }]
                    """),
            List.of(givenAcceptedAnnotation(OaMotivation.ODS_DELETING,
                new OaHasSelector().withAdditionalProperty("@type", "ods:FieldSelector")
                    .withAdditionalProperty("ods:field", "$['dcterms:description']"),
                null))
        ), Arguments.of(
            MAPPER.readTree(
                """
                      [{
                        "op": "copy",
                        "path": "/dcterms:rights",
                        "from": "/dcterms:license"
                      }]
                    """),
            List.of(givenAcceptedAnnotation(OaMotivation.ODS_ADDING,
                new OaHasSelector().withAdditionalProperty("@type", "ods:FieldSelector")
                    .withAdditionalProperty("ods:field", "$['dcterms:rights']"),
                new AnnotationBody().withOaValue(List.of(LICENSE_TESTVAL))
                    .withType("oa:TextualBody").withDctermsReferences(SOURCE_SYSTEM_ID)))
        ),
        Arguments.of(
            MAPPER.readTree(
                """
                      [{
                        "op": "move",
                        "path": "/dcterms:rights",
                        "from": "/dcterms:license"
                      }]
                    """),
            List.of(givenAcceptedAnnotation(OaMotivation.ODS_ADDING,
                    new OaHasSelector().withAdditionalProperty("@type", "ods:FieldSelector")
                        .withAdditionalProperty("ods:field", "$['dcterms:rights']"),
                    new AnnotationBody().withOaValue(List.of(LICENSE_TESTVAL))
                        .withType("oa:TextualBody").withDctermsReferences(SOURCE_SYSTEM_ID)),
                givenAcceptedAnnotation(OaMotivation.ODS_DELETING, new OaHasSelector()
                    .withAdditionalProperty("@type", "ods:FieldSelector")
                    .withAdditionalProperty("ods:field", "$['dcterms:license']"), null))
        ));
  }

  private static AnnotationProcessingRequest givenAcceptedAnnotation(OaMotivation motivation,
      OaHasSelector selector, AnnotationBody body) {
    var annotation = new AnnotationProcessingRequest()
        .withOaMotivation(motivation)
        .withOaHasBody(body)
        .withOaHasTarget(new AnnotationTarget()
            .withOdsType("ods:DigitalMedia")
            .withType(TYPE)
            .withId(DOI_PREFIX + HANDLE)
            .withOdsID(DOI_PREFIX + HANDLE)
            .withOaHasSelector(selector))
        .withDctermsCreated(Date.from(CREATED))
        .withDctermsCreator(
            new Agent().withType(AS_APPLICATION).withId(SOURCE_SYSTEM_ID)
                .withSchemaName(SOURCE_SYSTEM_NAME));
    if (motivation == OaMotivation.OA_EDITING) {
      annotation.withOaMotivatedBy("Received update information from Source System with id: "
          + SOURCE_SYSTEM_ID);
    }
    if (motivation == OaMotivation.ODS_ADDING) {
      annotation.withOaMotivatedBy("Received new information from Source System with id: "
          + SOURCE_SYSTEM_ID);
    }
    if (motivation == OaMotivation.ODS_DELETING) {
      annotation.withOaMotivatedBy("Received delete information from Source System with id: "
          + SOURCE_SYSTEM_ID);
    }
    return annotation;
  }

  @BeforeEach
  void setup() {
    service = new AnnotationPublisherService(kafkaPublisherService, applicationProperties, MAPPER);
    Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
    Instant instant = Instant.now(clock);
    mockedInstant = mockStatic(Instant.class);
    mockedInstant.when(Instant::now).thenReturn(instant);
    mockedInstant.when(() -> Instant.from(any())).thenReturn(instant);
    mockedInstant.when(() -> Instant.parse(any())).thenReturn(instant);
    mockedClock = mockStatic(Clock.class);
    mockedClock.when(Clock::systemUTC).thenReturn(clock);
  }

  @AfterEach
  void destroy() {
    mockedInstant.close();
    mockedClock.close();
  }

  @Test
  void testPublishAnnotationNewMedia() throws JsonProcessingException {
    // Given
    given(applicationProperties.getPid()).willReturn(APP_HANDLE);
    given(applicationProperties.getName()).willReturn(APP_NAME);

    // When
    service.publishAnnotationNewMedia(Set.of(givenDigitalMediaRecord()));

    // Then
    then(kafkaPublisherService).should()
        .publishAcceptedAnnotation(givenAutoAcceptedAnnotation(givenNewAcceptedAnnotation()));
  }

  @ParameterizedTest
  @MethodSource("provideUpdateAnnotations")
  void testPublishAnnotationUpdatedMedia(JsonNode jsonPatch,
      List<AnnotationProcessingRequest> expectedAnnotations) throws JsonProcessingException {
    // Given
    given(applicationProperties.getPid()).willReturn(APP_HANDLE);
    given(applicationProperties.getName()).willReturn(APP_NAME);

    // When
    service.publishAnnotationUpdatedMedia(
        Set.of(new UpdatedDigitalMediaRecord(givenDigitalMediaRecord(),
            List.of(), null, jsonPatch)));

    // Then
    for (var expectedAnnotation : expectedAnnotations) {
      then(kafkaPublisherService).should()
          .publishAcceptedAnnotation(givenAutoAcceptedAnnotation(expectedAnnotation));
    }
  }

  @Test
  void testInvalidCopy() throws JsonProcessingException {
    // Given
    var jsonPatch = MAPPER.readTree(
        """
              [{
                "op": "copy",
                "path": "/ods:sourceSystemName",
                "from": "/ods:someUnknownTerm"
              }]
            """);

    // When
    service.publishAnnotationUpdatedMedia(
        Set.of(new UpdatedDigitalMediaRecord(givenDigitalMediaRecord(),
            List.of(), null, jsonPatch)));

    // Then
    then(kafkaPublisherService).shouldHaveNoInteractions();
  }
}