package eu.dissco.core.digitalmediaobjectprocessor.service;

import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.CREATED;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.DIGITAL_SPECIMEN_ID;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.DIGITAL_SPECIMEN_ID_2;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.FORMAT;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.FORMAT_2;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.HANDLE;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MAPPER;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MEDIA_URL_1;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MEDIA_URL_2;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.PHYSICAL_SPECIMEN_ID_2;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.TYPE;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenDigitalMediaObject;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenDigitalMediaObjectRecord;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.DIGITAL_OBJECT_TYPE;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.FDO_PROFILE;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.ISSUED_FOR_AGENT;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.MEDIA_URL;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.REFERENT_NAME;
import static eu.dissco.core.digitalmediaobjectprocessor.domain.FdoProfileAttributes.SUBJECT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

  FdoRecordService fdoRecordService;

  private final Instant instant = Instant.now(Clock.fixed(CREATED, ZoneOffset.UTC));
  private MockedStatic<Instant> mockedStatic;

  @BeforeEach
  void setup() {
    fdoRecordService = new FdoRecordService(MAPPER);
    mockedStatic = mockStatic(Instant.class);
    mockedStatic.when(Instant::now).thenReturn(instant);
  }

  @AfterEach
  void destroy() {
    mockedStatic.close();
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
    var result = fdoRecordService.buildRollbackUpdateRequest(
        List.of(givenDigitalMediaObjectRecord()));

    // Then
    assertThat(result).isEqualTo(List.of(expected));
  }

  @Test
  void testBuildPostHandleRequest() throws Exception {
    // Given
    var expectedResponse = List.of(expectedPostRequest());

    // When
    var result = fdoRecordService.buildPostHandleRequest(List.of(givenDigitalMediaObject()));

    // Then
    assertThat(result).isEqualTo(expectedResponse);
  }

  @Test
  void testBuildPatchHandleRequest() throws Exception {
    // Given
    var requestBody = (ObjectNode) expectedPostRequest();
    var targetRecord = givenDigitalMediaObjectRecord();
    requestBody.put("id", targetRecord.id());
    var expectedResponse = List.of(requestBody);

    // When
    var result = fdoRecordService.buildPatchHandleRequest(List.of(targetRecord));

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

  @Test
  void testHandleDoesNeedUpdate() throws Exception {
    // Then
    assertThat(fdoRecordService.handleNeedsUpdate(givenDigitalMediaObject(),
        givenDigitalMediaObject(DIGITAL_SPECIMEN_ID_2, PHYSICAL_SPECIMEN_ID, FORMAT, MEDIA_URL_1,
            TYPE))).isTrue();
  }

  private static JsonNode expectedRollbackCreationRequest() throws Exception {
    return MAPPER.readTree("""
        {
          "data":[
          {"id":"20.5000.1025/1BY-BHB-AVN"}
          ]
        }
        """);
  }

  private static JsonNode expectedPostRequest() {
    var result = MAPPER.createObjectNode();
    var data = MAPPER.createObjectNode();
    var attributes = givenPostAttributes();
    data.put("type", "mediaObject");
    data.set("attributes", attributes);
    result.set("data", data);
    return result;
  }

  private static JsonNode givenPostAttributes() {
    var attributes = MAPPER.createObjectNode();
    attributes.put(FDO_PROFILE.getAttribute(), FDO_PROFILE.getDefaultValue());
    attributes.put(DIGITAL_OBJECT_TYPE.getAttribute(), DIGITAL_OBJECT_TYPE.getDefaultValue());
    attributes.put(ISSUED_FOR_AGENT.getAttribute(), ISSUED_FOR_AGENT.getDefaultValue());
    attributes.put(REFERENT_NAME.getAttribute(), TYPE + " for " + DIGITAL_SPECIMEN_ID);
    attributes.put("mediaHash", "");
    attributes.put("mediaHashAlgorithm", "");
    attributes.put("subjectSpecimenHost", "");
    attributes.put(MEDIA_URL.getAttribute(), MEDIA_URL_1);
    attributes.put(SUBJECT_ID.getAttribute(), DIGITAL_SPECIMEN_ID);
    return attributes;
  }


}
