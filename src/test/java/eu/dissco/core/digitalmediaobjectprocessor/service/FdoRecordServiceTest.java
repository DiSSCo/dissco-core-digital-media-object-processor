package eu.dissco.core.digitalmediaobjectprocessor.service;

import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.CREATED;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.DIGITAL_SPECIMEN_ID;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.DIGITAL_SPECIMEN_ID_2;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.FORMAT;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.HANDLE;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MAPPER;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MEDIA_URL_1;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.TYPE;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenDigitalMediaObject;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenDigitalMediaObjectRecord;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenPostAttributes;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenPostHandleRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObject;
import eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FdoRecordServiceTest {

  private final Instant instant = Instant.now(Clock.fixed(CREATED, ZoneOffset.UTC));
  FdoRecordService fdoRecordService;
  private MockedStatic<Instant> mockedStatic;
  private MockedStatic<Clock> mockedClock;

  private static JsonNode expectedRollbackCreationRequest() throws Exception {
    return MAPPER.readTree("""
        {
          "data":[
          {"id":"20.5000.1025/1BY-BHB-AVN"}
          ]
        }
        """);
  }

  @BeforeEach
  void setup() {
    Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
    fdoRecordService = new FdoRecordService(MAPPER);
    mockedStatic = mockStatic(Instant.class);
    mockedStatic.when(Instant::now).thenReturn(instant);
    mockedClock = mockStatic(Clock.class);
    mockedClock.when(Clock::systemUTC).thenReturn(clock);
  }

  @AfterEach
  void destroy() {
    mockedStatic.close();
    mockedClock.close();
  }

  @Test
  void testRollbackUpdate() throws Exception {
    var expectedString = """
        {
          "data": {
            "type": "mediaObject",
            "id":\"""" + HANDLE + "\","
        + "\"attributes\":" + givenPostAttributes().toPrettyString() + "}}";

    var expected = MAPPER.readTree(expectedString);
    // When
    var result = fdoRecordService.buildPatchDeleteRequest(
        List.of(givenDigitalMediaObjectRecord()));

    // Then
    assertThat(result).isEqualTo(List.of(expected));
  }

  @Test
  void testBuildPostHandleRequest() throws Exception {
    // Given
    var expectedResponse = List.of(givenPostHandleRequest());

    // When
    var result = fdoRecordService.buildPostHandleRequest(List.of(givenDigitalMediaObject()));

    // Then
    assertThat(result).isEqualTo(expectedResponse);
  }

  @Test
  void testBuildRollbackCreationRequest() throws Exception {
    // Given
    var expected = expectedRollbackCreationRequest();

    // When
    var result = fdoRecordService.buildRollbackCreationRequest(
        List.of(givenDigitalMediaObjectRecord()));

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testHandleDoesNotNeedUpdate() throws Exception {
    // Then
    assertThat(fdoRecordService.handleNeedsUpdate(givenDigitalMediaObject(),
        givenDigitalMediaObject())).isFalse();
  }

//  @Test
//  void testHandleDoesNeedUpdateLicense() throws Exception {
//    var attributes = MAPPER.readTree("""
//        {
//          "ac:accessURI":"http://data.rbge.org.uk/living/19942272",
//          "dcterms:license":"Different License",
//          "ods:organisationId":"https://ror.org/0x123"
//        }
//        """);
//    var mediaObject = new DigitalMediaObject(TYPE, DIGITAL_SPECIMEN_ID, attributes, null);
//
//    // Then
//    assertThat(fdoRecordService.handleNeedsUpdate(givenDigitalMediaObject(), mediaObject)).isTrue();
//  }

  @Test
  void testHandleDoesNeedUpdateId() throws Exception {
    // Then
    assertThat(fdoRecordService.handleNeedsUpdate(givenDigitalMediaObject(),
        givenDigitalMediaObject(DIGITAL_SPECIMEN_ID_2, FORMAT, MEDIA_URL_1, TYPE))).isTrue();
  }

//  @Test
//  void testMissingMandatoryElements() throws Exception {
//    // Given
//    var attributes = MAPPER.readTree("""
//                "ac:accessURI":"http://data.rbge.org.uk/living/19942272",
//                "dcterms:license":"Different License"
//        """);
//    var mediaObject = new DigitalMediaObject(TYPE, DIGITAL_SPECIMEN_ID, attributes, null);
//
//    // Then
//    assertThrows(PidCreationException.class, () -> fdoRecordService.buildPostHandleRequest(List.of(mediaObject)));
//  }

//  @Test
//  void testHandleNeedsUpdateMediaUrl() throws Exception {
//    // Given
//    var attributes = MAPPER.readTree("""
//         {
//          "ac:accessURI":"different uri",
//          "dcterms:license":"http://creativecommons.org/licenses/by-nc/3.0/",
//          "ods:organisationId":"https://ror.org/0x123"
//        }
//        """);
//
//    var mediaObject = new DigitalMediaObject(TYPE, DIGITAL_SPECIMEN_ID, attributes, null);
//
//    // Then
//    assertThat(fdoRecordService.handleNeedsUpdate(mediaObject, givenDigitalMediaObject())).isTrue();
//  }

//  @Test
//  void testHandleNeedsUpdateType() throws Exception {
//    // Given
//    var attributesCurrent = MAPPER.readTree("""
//         {
//          "ac:accessURI":"http://data.rbge.org.uk/living/19942272",
//          "dcterms:license":"http://creativecommons.org/licenses/by-nc/3.0/",
//          "ods:organisationId":"https://ror.org/0x123",
//          "dcterms:type":"2dimage"
//        }
//        """);
//
//    var attributesNew = MAPPER.readTree("""
//         {
//          "ac:accessURI":"http://data.rbge.org.uk/living/19942272",
//          "dcterms:license":"http://creativecommons.org/licenses/by-nc/3.0/",
//          "ods:organisationId":"https://ror.org/0x123",
//          "dcterms:type":"2dimageObject"
//        }
//        """);
//
//    var mediaObjectCurrent = new DigitalMediaObject(TYPE, DIGITAL_SPECIMEN_ID, attributesCurrent, null);
//    var mediaObjectNew = new DigitalMediaObject(TYPE, DIGITAL_SPECIMEN_ID, attributesNew, null);
//
//    // Then
//    assertThat(fdoRecordService.handleNeedsUpdate(mediaObjectNew, mediaObjectCurrent)).isTrue();
//  }

  @Test
  void testHandleDoesNeedUpdateType() throws Exception {
    var mediaObject = givenDigitalMediaObject(DIGITAL_SPECIMEN_ID, FORMAT, MEDIA_URL_1,
        "differentType");

    // Then
    assertThat(fdoRecordService.handleNeedsUpdate(givenDigitalMediaObject(), mediaObject)).isTrue();
  }

//  @Test
//  void testDcTermsType() throws Exception {
//    // Given
//    var attributes = MAPPER.readTree("""
//         {
//          "ac:accessURI":"http://data.rbge.org.uk/living/19942272",
//          "dcterms:license":"http://creativecommons.org/licenses/by-nc/3.0/",
//          "ods:organisationId":"https://ror.org/0x123",
//          "dcterms:type":"2dimageObject"
//        }
//        """);
//
//    var mediaObject = new DigitalMediaObject(TYPE, DIGITAL_SPECIMEN_ID, attributes, null);
//
//    // When
//    var result = fdoRecordService.buildPostHandleRequest(List.of(mediaObject)).get(0);
//
//    // Then
//    assertThat(
//        result.get("data").get("attributes").get(FdoProfileAttributes.MEDIA_FORMAT.getAttribute())
//            .asText()).isEqualTo(FdoProfileAttributes.MEDIA_FORMAT.getDefaultValue());
//  }
}
