package eu.dissco.core.digitalmediaprocessor;

import static eu.dissco.core.digitalmediaprocessor.domain.AgentRoleType.PROCESSING_SERVICE;
import static eu.dissco.core.digitalmediaprocessor.domain.AgentRoleType.SOURCE_SYSTEM;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.LICENSE_URL;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.LINKED_DO_PID;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.LINKED_DO_TYPE;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.MEDIA_HOST;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.MEDIA_HOST_NAME;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.MEDIA_ID;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.MEDIA_ID_NAME;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.MEDIA_ID_TYPE;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.MEDIA_TYPE;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.MIME_TYPE;
import static eu.dissco.core.digitalmediaprocessor.domain.FdoProfileAttributes.REFERENT_NAME;
import static eu.dissco.core.digitalmediaprocessor.schema.Agent.Type.SCHEMA_SOFTWARE_APPLICATION;
import static eu.dissco.core.digitalmediaprocessor.schema.Identifier.DctermsType.DOI;
import static eu.dissco.core.digitalmediaprocessor.utils.AgentUtils.createMachineAgent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalmediaprocessor.domain.AutoAcceptedAnnotation;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaEvent;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaKey;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaWrapper;
import eu.dissco.core.digitalmediaprocessor.schema.AnnotationBody;
import eu.dissco.core.digitalmediaprocessor.schema.AnnotationProcessingRequest;
import eu.dissco.core.digitalmediaprocessor.schema.AnnotationProcessingRequest.OaMotivation;
import eu.dissco.core.digitalmediaprocessor.schema.AnnotationTarget;
import eu.dissco.core.digitalmediaprocessor.schema.DigitalMedia;
import eu.dissco.core.digitalmediaprocessor.schema.EntityRelationship;
import eu.dissco.core.digitalmediaprocessor.schema.Identifier.DctermsType;
import eu.dissco.core.digitalmediaprocessor.schema.OaHasSelector;
import eu.dissco.core.digitalmediaprocessor.utils.DigitalMediaUtils;
import java.time.Instant;
import java.util.Date;
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

  public static DigitalMediaEvent givenDigitalMediaEventWithMediaId()
      throws JsonProcessingException {
    return new DigitalMediaEvent(
        List.of("OCR"),
        givenDigitalMediaWrapperWithMediaId()
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

  private static DigitalMediaWrapper givenDigitalMediaWrapperWithMediaId()
      throws JsonProcessingException {
    return new DigitalMediaWrapper(
        TYPE,
        DIGITAL_SPECIMEN_ID,
        generateAttributes(FORMAT, TestUtils.MEDIA_URL_1)
            .withId(HANDLE)
            .withDctermsIdentifier(HANDLE),
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

  public static DigitalMediaRecord givenDigitalMediaRecordWithId()
      throws JsonProcessingException {
    return new DigitalMediaRecord(
        HANDLE,
        VERSION,
        CREATED,
        givenDigitalMediaWrapperWithMediaId()
    );
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

  public static DigitalMediaRecord givenDigitalMediaRecordNoOriginalData() {
    return new DigitalMediaRecord(
        HANDLE,
        VERSION,
        CREATED,
        givenDigitalMediaWrapperNoOriginalData()
    );
  }

  public static DigitalMediaWrapper givenDigitalMediaWrapper() throws JsonProcessingException {
    return givenDigitalMediaWrapper(DIGITAL_SPECIMEN_ID, FORMAT, MEDIA_URL_1,
        TYPE);
  }

  public static DigitalMediaWrapper givenDigitalMediaWrapper(String digitalSpecimenId,
      String format, String mediaUrl, String type) throws JsonProcessingException {
    return new DigitalMediaWrapper(
        type,
        digitalSpecimenId,
        generateAttributes(format, mediaUrl),
        generateOriginalAttributes()
    );
  }

  public static DigitalMediaWrapper givenDigitalMediaWrapperNoOriginalData() {
    return new DigitalMediaWrapper(
        TYPE,
        DIGITAL_SPECIMEN_ID,
        generateAttributes(FORMAT, MEDIA_URL_1),
        MAPPER.createObjectNode()
    );
  }

  public static DigitalMedia generateAttributes(String format, String mediaUrl) {
    return new DigitalMedia()
        .withAcAccessURI(mediaUrl)
        .withDctermsFormat(format)
        .withDctermsRights(LICENSE_TESTVAL)
        .withOdsOrganisationID(MEDIA_HOST_TESTVAL)
        .withDctermsModified("2022-09-16T08:52:27.391Z")
        .withOdsSourceSystemID(SOURCE_SYSTEM_ID)
        .withOdsSourceSystemName(SOURCE_SYSTEM_NAME)
        .withOdsHasEntityRelationships(List.of(
            new EntityRelationship().withType("hasDigitalSpecimen")
                .withType("ods:EntityRelationship")
                .withDwcRelationshipEstablishedDate(Date.from(CREATED))
                .withDwcRelationshipOfResource("hasDigitalSpecimen")
                .withOdsHasAgents(
                    List.of(createMachineAgent(APP_NAME, APP_HANDLE, PROCESSING_SERVICE,
                        DOI, SCHEMA_SOFTWARE_APPLICATION)))
                .withDwcRelatedResourceID(DOI_PREFIX + DIGITAL_SPECIMEN_ID)));
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
    return MAPPER.createObjectNode()
        .set("data", MAPPER.createObjectNode()
            .put("type", "https://doi.org/21.T11148/bbad8c4e101e8af01115")
            .set("attributes", givenPostHandleAttributes()));
  }

  public static JsonNode givenPostHandleAttributes() {
    return MAPPER.createObjectNode()
        .put(REFERENT_NAME.getAttribute(), MEDIA_URL_1)
        .put(MEDIA_HOST.getAttribute(), MEDIA_HOST_TESTVAL)
        .put(MEDIA_HOST_NAME.getAttribute(), (String) null)
        .put(LINKED_DO_PID.getAttribute(), DIGITAL_SPECIMEN_ID)
        .put(LINKED_DO_TYPE.getAttribute(), "https://doi.org/21.T11148/894b1e6cad57e921764e")
        .put(MEDIA_ID.getAttribute(), MEDIA_URL_1)
        .put(MEDIA_ID_TYPE.getAttribute(), "Resolvable")
        .put(MEDIA_ID_NAME.getAttribute(), "ac:accessURI")
        .put(MEDIA_TYPE.getAttribute(), TYPE)
        .put(MIME_TYPE.getAttribute(), FORMAT)
        .put(LICENSE_URL.getAttribute(), LICENSE_TESTVAL);
  }

  public static AutoAcceptedAnnotation givenAutoAcceptedAnnotation(
      AnnotationProcessingRequest annotation) {
    return new AutoAcceptedAnnotation(
        createMachineAgent(APP_NAME, APP_HANDLE, PROCESSING_SERVICE, DOI,
            SCHEMA_SOFTWARE_APPLICATION), annotation);
  }

  public static AnnotationProcessingRequest givenNewAcceptedAnnotation()
      throws JsonProcessingException {
    return new AnnotationProcessingRequest()
        .withOaMotivation(OaMotivation.ODS_ADDING)
        .withOaMotivatedBy("New information received from Source System with id: "
            + SOURCE_SYSTEM_ID)
        .withOaHasBody(new AnnotationBody()
            .withType("oa:TextualBody")
            .withOaValue(List.of(MAPPER.writeValueAsString(
                DigitalMediaUtils.flattenToDigitalMedia(givenDigitalMediaRecord()))))
            .withDctermsReferences(SOURCE_SYSTEM_ID))
        .withDctermsCreated(Date.from(CREATED))
        .withDctermsCreator(createMachineAgent(SOURCE_SYSTEM_NAME, SOURCE_SYSTEM_ID, SOURCE_SYSTEM,
            DctermsType.HANDLE, SCHEMA_SOFTWARE_APPLICATION))
        .withOaHasTarget(new AnnotationTarget()
            .withId(DOI_PREFIX + HANDLE)
            .withDctermsIdentifier(DOI_PREFIX + HANDLE)
            .withType(TYPE)
            .withOdsFdoType("ods:DigitalMedia")
            .withOaHasSelector(new OaHasSelector()
                .withAdditionalProperty("@type", "ods:ClassSelector")
                .withAdditionalProperty("ods:class", "$")));

  }

  public static JsonNode givenJsonPatch() throws JsonProcessingException {
    return MAPPER.readTree(
        "[{\"op\":\"replace\",\"path\":\"/dcterms:format\",\"value\":\"image/jpeg\"}]");
  }

}
