package eu.dissco.core.digitalmediaprocessor.service;

import static eu.dissco.core.digitalmediaprocessor.TestUtils.CREATED;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.DIGITAL_SPECIMEN_ID;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.DIGITAL_SPECIMEN_ID_2;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.FORMAT;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.HANDLE;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.MAPPER;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.MEDIA_URL_1;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.TYPE;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.generateAttributes;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenDigitalMediaWrapper;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenPostHandleAttributes;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenPostHandleRequest;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.LICENSE_ID;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.LICENSE_NAME;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.RIGHTS_HOLDER_ID;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.RIGHTS_HOLDER_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;

import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.core.digitalmediaprocessor.TestUtils;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaWrapper;
import eu.dissco.core.digitalmediaprocessor.properties.FdoProperties;
import eu.dissco.core.digitalmediaprocessor.schema.DigitalMedia;
import eu.dissco.core.digitalmediaprocessor.schema.DigitalMedia.DctermsType;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FdoRecordServiceTest {

  private final Instant instant = Instant.now(Clock.fixed(CREATED, ZoneOffset.UTC));
  FdoRecordService fdoRecordService;
  private MockedStatic<Instant> mockedStatic;
  private MockedStatic<Clock> mockedClock;

  @BeforeEach
  void setup() {
    Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
    fdoRecordService = new FdoRecordService(MAPPER, new FdoProperties());
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
            "type": "https://doi.org/21.T11148/bbad8c4e101e8af01115",
            "id":\"""" + HANDLE + "\","
        + "\"attributes\":" + givenPostHandleAttributes().toPrettyString() + "}}";

    var expected = MAPPER.readTree(expectedString);
    // When
    var result = fdoRecordService.buildPatchDeleteRequest(
        List.of(TestUtils.givenDigitalMediaRecord()));

    // Then
    assertThat(result).isEqualTo(List.of(expected));
  }

  @Test
  void testBuildPostHandleRequest() throws Exception {
    // Given
    var expected = List.of(givenPostHandleRequest());

    // When
    var result = fdoRecordService.buildPostHandleRequest(List.of(givenDigitalMediaWrapper()));

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("genLicenseAndRightsHolder")
  void testLicenseAndRightsHolder(String licenseField, String rightsHolderField,
      String fieldValue) {
    // Given
    var expectedAttributes = ((ObjectNode) givenPostHandleAttributes());
    expectedAttributes.remove(LICENSE_ID.getAttribute());
    expectedAttributes
        .put(licenseField, fieldValue)
        .put(rightsHolderField, fieldValue);
    var expected = List.of(MAPPER.createObjectNode()
        .set("data", MAPPER.createObjectNode()
            .put("type", "https://doi.org/21.T11148/bbad8c4e101e8af01115")
            .set("attributes", expectedAttributes)));
    var media = new DigitalMediaWrapper(
        TYPE, DIGITAL_SPECIMEN_ID,
        generateAttributes(FORMAT, MEDIA_URL_1)
            .withDctermsLicense(fieldValue)
            .withDctermsRightsHolder(fieldValue),
        MAPPER.createObjectNode()
    );

    // When
    var result = fdoRecordService.buildPostHandleRequest(List.of(media));

    // Then
    assertThat(result).isEqualTo(expected);
  }

  static Stream<Arguments> genLicenseAndRightsHolder() {
    return Stream.of(
        Arguments.of(LICENSE_ID.getAttribute(), RIGHTS_HOLDER_ID.getAttribute(),
            "https://spdx.org/licenses/Apache-2.0.html"),
        Arguments.of(LICENSE_NAME.getAttribute(), RIGHTS_HOLDER_NAME.getAttribute(), "Apache 2.0"));
  }

  @Test
  void testBuildRollbackCreationRequest() throws Exception {
    // Given
    var expected = List.of(HANDLE);

    // When
    var result = fdoRecordService.buildRollbackCreationRequest(
        List.of(TestUtils.givenDigitalMediaRecord()));

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testHandleDoesNotNeedUpdate() throws Exception {
    // Then
    assertThat(fdoRecordService.handleNeedsUpdate(TestUtils.givenDigitalMediaWrapper(),
        TestUtils.givenDigitalMediaWrapper())).isFalse();
  }

  @Test
  void testHandleDoesNeedUpdateLicense() throws Exception {
    var attributes = new DigitalMedia()
        .withDctermsLicense("http://data.rbge.org.uk/living/19942272")
        .withDctermsLicense("Different License")
        .withOdsOrganisationID("https://ror.org/0x123");
    var mediaObject = new DigitalMediaWrapper(TYPE, DIGITAL_SPECIMEN_ID, attributes, null);

    // Then
    assertThat(fdoRecordService.handleNeedsUpdate(TestUtils.givenDigitalMediaWrapper(),
        mediaObject)).isTrue();
  }

  @Test
  void testHandleDoesNeedUpdateId() throws Exception {
    // Then
    assertThat(fdoRecordService.handleNeedsUpdate(TestUtils.givenDigitalMediaWrapper(),
        givenDigitalMediaWrapper(DIGITAL_SPECIMEN_ID_2, FORMAT, MEDIA_URL_1, TYPE))).isTrue();
  }

  @Test
  void testHandleNeedsUpdateMediaUrl() throws Exception {
    // Given
    var attributes = new DigitalMedia()
        .withAcAccessURI("different uri")
        .withDctermsLicense("http://creativecommons.org/licenses/by-nc/3.0/")
        .withOdsOrganisationID("https://ror.org/0x123");

    // When
    var mediaObject = new DigitalMediaWrapper(TYPE, DIGITAL_SPECIMEN_ID, attributes, null);

    // Then
    assertThat(fdoRecordService.handleNeedsUpdate(mediaObject,
        TestUtils.givenDigitalMediaWrapper())).isTrue();
  }

  @Test
  void testHandleNeedsUpdateType() {
    // Given
    var attributesCurrent = new DigitalMedia()
        .withAcAccessURI("http://data.rbge.org.uk/living/19942272")
        .withDctermsLicense("http://creativecommons.org/licenses/by-nc/3.0/")
        .withOdsOrganisationID("https://ror.org/0x123")
        .withDctermsType(DctermsType.IMAGE);

    var attributesNew = new DigitalMedia()
        .withAcAccessURI("http://data.rbge.org.uk/living/19942272")
        .withDctermsLicense("http://creativecommons.org/licenses/by-nc/3.0/")
        .withOdsOrganisationID("https://ror.org/0x123")
        .withDctermsType(DctermsType.MOVING_IMAGE);

    var mediaObjectCurrent = new DigitalMediaWrapper(TYPE, DIGITAL_SPECIMEN_ID, attributesCurrent,
        null);
    var mediaObjectNew = new DigitalMediaWrapper(TYPE, DIGITAL_SPECIMEN_ID, attributesNew, null);

    // When / Then
    assertThat(fdoRecordService.handleNeedsUpdate(mediaObjectNew, mediaObjectCurrent)).isTrue();
  }

  @Test
  void testHandleDoesNeedUpdateType() throws Exception {
    var mediaObject = givenDigitalMediaWrapper(DIGITAL_SPECIMEN_ID, FORMAT, MEDIA_URL_1,
        "differentType");

    // Then
    assertThat(fdoRecordService.handleNeedsUpdate(TestUtils.givenDigitalMediaWrapper(),
        mediaObject)).isTrue();
  }
}
