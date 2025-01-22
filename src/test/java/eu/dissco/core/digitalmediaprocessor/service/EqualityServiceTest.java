package eu.dissco.core.digitalmediaprocessor.service;


import static eu.dissco.core.digitalmediaprocessor.TestUtils.DIGITAL_SPECIMEN_ID;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.FORMAT;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.FORMAT_2;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.MAPPER;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.MEDIA_URL_1;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.TYPE;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.UPDATED_TIMESTAMP;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.generateAttributes;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.generateOriginalAttributes;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenDigitalMediaWrapper;
import static org.assertj.core.api.Assertions.assertThat;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Option;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaEvent;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaWrapper;
import java.sql.Date;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class EqualityServiceTest {

  private EqualityService equalityService;
  private static final Configuration JSON_PATH_CONFIG = Configuration.builder()
      .options(Option.AS_PATH_LIST, Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST)
      .build();

  @BeforeEach
  void setup() {
    equalityService = new EqualityService(JSON_PATH_CONFIG, MAPPER);
  }

  @ParameterizedTest
  @MethodSource("provideUnequalMedia")
  void testIsUnequal(DigitalMediaWrapper currentDigitalMedia, DigitalMediaWrapper digitalMedia) {
    // When
    var result = equalityService.isEqual(currentDigitalMedia, digitalMedia);

    // Then
    assertThat(result).isFalse();
  }

  @ParameterizedTest
  @MethodSource("provideEqualMedia")
  void testIsEqual(DigitalMediaWrapper currentDigitalMedia, DigitalMediaWrapper digitalMedia){
    // When
    var result = equalityService.isEqual(currentDigitalMedia, digitalMedia);

    // Then
    assertThat(result).isTrue();
  }

  @Test
  void testSetDates() throws Exception {
    // Given
    var currentDigitalMedia = givenDigitalMediaRecord();
    var digitalMedia = new DigitalMediaEvent(
        List.of(), changeDates(givenDigitalMediaWrapper()));
    var expected = new DigitalMediaEvent(
        List.of(),
        new DigitalMediaWrapper(
        digitalMedia.digitalMediaWrapper().type(),
        digitalMedia.digitalMediaWrapper().digitalSpecimenID(),
            generateAttributes(FORMAT, MEDIA_URL_1)
                .withDctermsModified(UPDATED_TIMESTAMP.toString()),
        generateOriginalAttributes()
    ));

    // When
    var result = equalityService.setEventDates(currentDigitalMedia, digitalMedia);

    assertThat(result).isEqualTo(expected);
  }

  private static Stream<Arguments> provideUnequalMedia() throws Exception {
    return Stream.of(
        Arguments.of(givenDigitalMediaRecord(FORMAT_2).digitalMediaWrapper(), givenDigitalMediaWrapper()),
        Arguments.of(null, givenDigitalMediaWrapper()),
        Arguments.of(new DigitalMediaWrapper(TYPE, DIGITAL_SPECIMEN_ID, null, generateOriginalAttributes()), givenDigitalMediaWrapper())
    );
  }

  private static Stream<Arguments> provideEqualMedia() throws Exception {
    return Stream.of(
        Arguments.of(givenDigitalMediaWrapper(), givenDigitalMediaWrapper()),
        Arguments.of(givenDigitalMediaWrapper(), changeDates(givenDigitalMediaWrapper())),
        Arguments.of(givenDigitalMediaWrapper(), changeDates(
            new DigitalMediaWrapper(
                TYPE,
                DIGITAL_SPECIMEN_ID,
                changeDates(givenDigitalMediaWrapper()).attributes(),
                MAPPER.createObjectNode()
        )))
    );
  }


  private static DigitalMediaWrapper changeDates(DigitalMediaWrapper digitalMediaWrapper) {
    var media = digitalMediaWrapper.attributes();
    media
        .withDctermsCreated(Date.from(UPDATED_TIMESTAMP))
        .withDctermsModified(UPDATED_TIMESTAMP.toString());
    media.getOdsHasEntityRelationships().forEach(
        er -> er.withDwcRelationshipEstablishedDate(Date.from(UPDATED_TIMESTAMP))
    );
    return digitalMediaWrapper;

  }


}
