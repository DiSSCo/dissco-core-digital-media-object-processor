package eu.dissco.core.digitalmediaprocessor;

import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.ISSUED_FOR_AGENT;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.IS_DERIVED_FROM_SPECIMEN;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.LICENSE_NAME;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.LINKED_DO_PID;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.LINKED_DO_TYPE;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.MEDIA_FORMAT;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.MEDIA_HOST;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.PRIMARY_MEDIA_ID;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.PRIMARY_MO_ID_NAME;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.PRIMARY_MO_ID_TYPE;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.DCTERMS_TYPE;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.REFERENT_NAME;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.RIGHTSHOLDER_PID_TYPE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaEvent;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaKey;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaWrapper;
import eu.dissco.core.digitalmediaprocessor.schema.DigitalMedia;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;

public class TestUtils {

  public static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

  public static final String HANDLE = "20.5000.1025/1BY-BHB-AVN";
  public static final String HANDLE_2 = "20.5000.1025/1BY-BHB-XXX";
  public static final String HANDLE_3 = "20.5000.1025/1BY-BHB-YYY";
  public static final String DOI_PREFIX = "https://doi.org/";
  public static final int VERSION = 1;
  public static final Instant CREATED = Instant.parse("2022-09-16T08:52:27.391Z");
  public static final Instant UPDATED_TIMESTAMP = Instant.parse("2023-03-23T15:41:27.391Z");
  public static final String TYPE = "Image";
  public static final String MAS = "OCR";
  public static final String DIGITAL_SPECIMEN_ID = "20.5000.1025/460-A7R-QMJ";
  public static final String DIGITAL_SPECIMEN_ID_2 = "20.5000.1025/460-A7R-XXX";
  public static final String DIGITAL_SPECIMEN_ID_3 = "20.5000.1025/460-A7R-YYY";
  public static final String MEDIA_URL_1 = "http://data.rbge.org.uk/living/19942272";
  public static final String MEDIA_URL_2 = "http://data.rbge.org.uk/living/1994227X";
  public static final String MEDIA_URL_3 = "http://data.rbge.org.uk/living/1994227Y";
  public static final String FORMAT = "image/jpeg";
  public static final String FORMAT_2 = "image/png";
  public static final String MEDIA_HOST_TESTVAL = "https://ror.org/0x123";
  public static final String LICENSE_TESTVAL = "http://creativecommons.org/licenses/by-nc/3.0/";
  public static final String SOURCE_SYSTEM_ID = "https://hdl.handle.net/TEST/57Z-6PC-64W";
  public static final String SOURCE_SYSTEM_NAME = "A very nice source system";
  public static final String APP_HANDLE = "https://hdl.handle.net/TEST/123-123-123";
  public static final String APP_NAME = "dissco-digital-specimen-processor";

  public static DigitalMediaEvent givenMediaEvent() throws JsonProcessingException {
    return new DigitalMediaEvent(
        List.of("OCR"), givenDigitalMediaWrapper(DIGITAL_SPECIMEN_ID, MEDIA_URL_1)
    );
  }

  public static DigitalMediaEvent givenDigitalMediaEvent()
      throws JsonProcessingException {
    return givenDigitalMediaEvent(DIGITAL_SPECIMEN_ID, MEDIA_URL_1);
  }

  public static DigitalMediaEvent givenDigitalMediaEvent(
      String digitalSpecimenId, String mediaUrl)
      throws JsonProcessingException {
    return new DigitalMediaEvent(
        List.of(MAS),
        givenDigitalMediaWrapper(digitalSpecimenId, mediaUrl)
    );
  }

  public static HashMap<DigitalMediaKey, String> givenPidMap(int size) {
    HashMap<DigitalMediaKey, String> pidMap = new HashMap<>();
    pidMap.put(givenDigitalMediaKey(), HANDLE);
    size = size - 1;
    if (size > 0) {
      pidMap.put(new DigitalMediaKey(DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2), HANDLE_2);
    }
    size = size - 1;
    if (size > 0) {
      pidMap.put(new DigitalMediaKey(DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3), HANDLE_3);
    }
    return pidMap;
  }

  public static DigitalMediaKey givenDigitalMediaKey() {
    return new DigitalMediaKey(
        DIGITAL_SPECIMEN_ID,
        MEDIA_URL_1
    );
  }

  private static DigitalMediaWrapper givenDigitalMediaWrapper(String digitalSpecimenId,
      String mediaUrl)
      throws JsonProcessingException {
    return new DigitalMediaWrapper(
        TYPE,
        digitalSpecimenId,
        generateAttributes(FORMAT, mediaUrl),
        generateOriginalAttributes()
    );
  }

  public static DigitalMediaRecord givenDigitalMediaRecordWithVersion(int version)
      throws JsonProcessingException {
    return new DigitalMediaRecord(
        HANDLE,
        version,
        CREATED,
        givenDigitalMediaWrapper()
    );
  }

  public static DigitalMediaRecord givenDigitalMediaRecord()
      throws JsonProcessingException {
    return givenDigitalMediaRecord(HANDLE, DIGITAL_SPECIMEN_ID, MEDIA_URL_1, FORMAT);
  }

  public static DigitalMediaRecord givenDigitalMediaRecordPhysical(
      String handle, String digitalSpecimenId, String mediaUrl, String type)
      throws JsonProcessingException {
    return new DigitalMediaRecord(
        handle,
        VERSION,
        CREATED,
        givenDigitalMediaWrapper(digitalSpecimenId, FORMAT, mediaUrl, type)
    );
  }

  public static DigitalMediaRecord givenDigitalMediaRecord(String format)
      throws JsonProcessingException {
    return givenDigitalMediaRecord(HANDLE, DIGITAL_SPECIMEN_ID, MEDIA_URL_1, format);
  }

  public static DigitalMediaRecord givenDigitalMediaRecord(String handle, String format)
      throws JsonProcessingException {
    return givenDigitalMediaRecord(handle, DIGITAL_SPECIMEN_ID, MEDIA_URL_1, format);
  }

  public static DigitalMediaRecord givenDigitalMediaRecord(String pid,
      String digitalSpecimenId, String mediaUrl)
      throws JsonProcessingException {
    return givenDigitalMediaRecord(pid, digitalSpecimenId, mediaUrl, FORMAT);
  }

  public static DigitalMediaRecord givenDigitalMediaRecord(String pid,
      String digitalSpecimenId, String mediaUrl, String format)
      throws JsonProcessingException {
    return new DigitalMediaRecord(
        pid,
        VERSION,
        CREATED,
        givenDigitalMediaWrapper(digitalSpecimenId, format, mediaUrl, TYPE)
    );
  }

  public static DigitalMediaWrapper givenDigitalMediaWrapper() throws JsonProcessingException {
    return givenDigitalMediaWrapper(DIGITAL_SPECIMEN_ID, FORMAT, MEDIA_URL_1,
        TYPE);
  }

  public static DigitalMediaWrapper givenDigitalMediaWrapper(String digitalSpecimenId, String format,
      String mediaUrl, String type) throws JsonProcessingException {
    return new DigitalMediaWrapper(
        type,
        digitalSpecimenId,
        generateAttributes(format, mediaUrl),
        generateOriginalAttributes()
    );
  }

  public static DigitalMedia generateAttributes(String format, String mediaUrl) {
    return new DigitalMedia()
        .withAcAccessURI(mediaUrl)
        .withDctermsFormat(format)
        .withDctermsLicense(LICENSE_TESTVAL)
        .withOdsOrganisationID(MEDIA_HOST_TESTVAL)
        .withDctermsModified("2022-09-16T08:52:27.391Z")
        .withOdsSourceSystemID(SOURCE_SYSTEM_ID);
  }

  private static JsonNode generateOriginalAttributes() throws JsonProcessingException {
    return MAPPER.readValue(
        """
             {
              "dwca:ID": "045db6cb-5f06-4c19-b0f6-9620bdff3ae4",
               "dcterms:type": "StillImage",
               "dcterms:title": "19942272",
               "dcterms:format": "image/jpeg",
               "dcterms:creator": "Unknown",
               "dcterms:license": "http://creativecommons.org/licenses/by-nc/3.0/",
               "dcterms:publisher": "Royal Botanic Garden Edinburgh",
               "dcterms:identifier": "http://repo.rbge.org.uk/image_server.php?kind=1500&path_base64=L2l0ZW1faW1hZ2VzL2FjY2Vzc2lvbnMvMTkvOTQvMjIvNzIvUGhvdG9fNTFjMWNlNzM5ZDE2Zi5qcGc=",
               "dcterms:references": "http://data.rbge.org.uk/living/19942272",
               "dcterms:description": "Image of living collection specimen with accession number 19942272 by Unknown",
               "dcterms:rightsHolder": "Copyright Royal Botanic Garden Edinburgh. Contact us for rights to commercial use."
            }
            """, JsonNode.class
    );
  }

  public static JsonNode givenPostHandleRequest() {
    var result = MAPPER.createObjectNode();
    var data = MAPPER.createObjectNode();
    var attributes = givenPostAttributes();
    data.put("type", "https://hdl.handle.net/21.T11148/bbad8c4e101e8af01115");
    data.set("attributes", attributes);
    result.set("data", data);
    return result;
  }

  public static JsonNode givenPostAttributes() {
    var attributes = MAPPER.createObjectNode();
    attributes.put(MEDIA_HOST.getAttribute(), MEDIA_HOST_TESTVAL);
    attributes.put(LICENSE_NAME.getAttribute(), LICENSE_TESTVAL);
    attributes.put(PRIMARY_MEDIA_ID.getAttribute(), MEDIA_URL_1);
    attributes.put(REFERENT_NAME.getAttribute(), TYPE + " for " + DIGITAL_SPECIMEN_ID);
    attributes.put(LINKED_DO_PID.getAttribute(), DIGITAL_SPECIMEN_ID);
    attributes.put(MEDIA_FORMAT.getAttribute(), "image");
    attributes.put(ISSUED_FOR_AGENT.getAttribute(), "https://ror.org/0566bfb96");
    attributes.put(DCTERMS_TYPE.getAttribute(), DCTERMS_TYPE.getDefaultValue());
    attributes.put(PRIMARY_MO_ID_TYPE.getAttribute(), PRIMARY_MO_ID_TYPE.getDefaultValue());
    attributes.put(PRIMARY_MO_ID_NAME.getAttribute(), PRIMARY_MO_ID_NAME.getDefaultValue());
    attributes.put(RIGHTSHOLDER_PID_TYPE.getAttribute(), RIGHTSHOLDER_PID_TYPE.getDefaultValue());
    attributes.put(IS_DERIVED_FROM_SPECIMEN.getAttribute(),
        Boolean.valueOf(IS_DERIVED_FROM_SPECIMEN.getDefaultValue()));
    attributes.put(LINKED_DO_TYPE.getAttribute(), LINKED_DO_TYPE.getDefaultValue());

    return attributes;
  }

}
