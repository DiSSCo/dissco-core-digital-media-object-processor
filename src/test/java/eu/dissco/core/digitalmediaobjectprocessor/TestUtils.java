package eu.dissco.core.digitalmediaobjectprocessor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObject;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectEvent;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectKey;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectRecord;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectTransfer;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectTransferEvent;
import eu.dissco.core.digitalmediaobjectprocessor.domain.UpdatedDigitalMediaTuple;
import java.time.Instant;
import java.util.List;

public class TestUtils {

  public static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

  public static final String HANDLE = "20.5000.1025/1BY-BHB-AVN";
  public static final String HANDLE_2 = "20.5000.1025/1BY-BHB-XXX";
  public static final String HANDLE_3 = "20.5000.1025/1BY-BHB-YYY";
  public static final int VERSION = 1;
  public static final Instant CREATED = Instant.parse("2022-09-16T08:52:27.391Z");
  public static final Instant UPDATED_TIMESTAMP = Instant.parse("2023-03-23T15:41:27.391Z");
  public static final String TYPE = "2DImageObject";
  public static final String AAS = "OCR";
  public static final String PHYSICAL_SPECIMEN_ID = "045db6cb-5f06-4c19-b0f6-9620bdff3ae4:040ck2b86";
  public static final String PHYSICAL_SPECIMEN_ID_2 = "045db6cb-5f06-4c19-b0f6-9620bdff3ae4:041ck2b86";
  public static final String PHYSICAL_SPECIMEN_ID_3 = "045db6cb-5f06-4c19-b0f6-9620bdff3ae4:042ck2b86";
  public static final String DIGITAL_SPECIMEN_ID = "20.5000.1025/460-A7R-QMJ";
  public static final String DIGITAL_SPECIMEN_ID_2 = "20.5000.1025/460-A7R-XXX";
  public static final String DIGITAL_SPECIMEN_ID_3 = "20.5000.1025/460-A7R-YYY";
  public static final String MEDIA_URL = "http://data.rbge.org.uk/living/19942272";
  public static final String MEDIA_URL_2 = "http://data.rbge.org.uk/living/1994227X";
  public static final String MEDIA_URL_3 = "http://data.rbge.org.uk/living/1994227Y";
  public static final String FORMAT = "image/jpeg";
  public static final String FORMAT_2 = "image/png";

  public static DigitalMediaObjectTransferEvent givenMediaEvent() throws JsonProcessingException {
    return new DigitalMediaObjectTransferEvent(
        List.of("OCR"), givenDigitalMediaTransfer(PHYSICAL_SPECIMEN_ID, MEDIA_URL)
    );
  }

  public static UpdatedDigitalMediaTuple givenUpdatedDigitalMediaTuple()
      throws JsonProcessingException {
    return new UpdatedDigitalMediaTuple(
        givenDigitalMediaObjectRecord(FORMAT_2),
        new DigitalMediaObjectEvent(
            List.of(AAS),
            givenDigitalMediaObject()
        )
    );
  }

  public static DigitalMediaObjectTransferEvent givenDigitalMediaObjectTransferEvent()
      throws JsonProcessingException {
    return givenDigitalMediaObjectTransferEvent(PHYSICAL_SPECIMEN_ID, MEDIA_URL);
  }

  public static DigitalMediaObjectTransferEvent givenDigitalMediaObjectTransferEvent(
      String physicalSpecimenId, String mediaUrl)
      throws JsonProcessingException {
    return new DigitalMediaObjectTransferEvent(
        List.of(AAS),
        givenDigitalMediaTransfer(physicalSpecimenId, mediaUrl)
    );
  }

  public static DigitalMediaObjectKey givenDigitalMediaKey() {
    return new DigitalMediaObjectKey(
        DIGITAL_SPECIMEN_ID,
        MEDIA_URL
    );
  }

  private static DigitalMediaObjectTransfer givenDigitalMediaTransfer(String physicalSpecimenId,
      String mediaUrl)
      throws JsonProcessingException {
    return new DigitalMediaObjectTransfer(
        TYPE,
        physicalSpecimenId,
        generateAttributes(FORMAT, mediaUrl),
        generateOriginalAttributes()
    );
  }

  public static DigitalMediaObjectRecord givenDigitalMediaObjectRecordWithVersion(int version)
      throws JsonProcessingException {
    return new DigitalMediaObjectRecord(
        HANDLE,
        version,
        CREATED,
        givenDigitalMediaObject()
    );
  }

  public static DigitalMediaObjectRecord givenDigitalMediaObjectRecord()
      throws JsonProcessingException {
    return givenDigitalMediaObjectRecord(HANDLE, DIGITAL_SPECIMEN_ID, MEDIA_URL, FORMAT);
  }

  public static DigitalMediaObjectRecord givenDigitalMediaObjectRecordPhysical(
      String handle, String physicalSpecimenId, String digitalSpecimenId, String mediaUrl, String type)
      throws JsonProcessingException {
    return new DigitalMediaObjectRecord(
        handle,
        VERSION,
        CREATED,
        givenDigitalMediaObject(digitalSpecimenId, physicalSpecimenId, FORMAT, mediaUrl, type)
    );
  }

  public static DigitalMediaObjectRecord givenDigitalMediaObjectRecord(String format)
      throws JsonProcessingException {
    return givenDigitalMediaObjectRecord(HANDLE, DIGITAL_SPECIMEN_ID, MEDIA_URL, format);
  }

  public static DigitalMediaObjectRecord givenDigitalMediaObjectRecord(String handle, String format)
      throws JsonProcessingException {
    return givenDigitalMediaObjectRecord(handle, DIGITAL_SPECIMEN_ID, MEDIA_URL, format);
  }

  public static DigitalMediaObjectRecord givenDigitalMediaObjectRecord(String pid,
      String digitalSpecimenId, String mediaUrl)
      throws JsonProcessingException {
    return givenDigitalMediaObjectRecord(pid, digitalSpecimenId, mediaUrl, FORMAT);
  }

  public static DigitalMediaObjectRecord givenDigitalMediaObjectRecord(String pid,
      String digitalSpecimenId, String mediaUrl, String format)
      throws JsonProcessingException {
    return new DigitalMediaObjectRecord(
        pid,
        VERSION,
        CREATED,
        givenDigitalMediaObject(digitalSpecimenId, PHYSICAL_SPECIMEN_ID, format, mediaUrl, TYPE)
    );
  }

  public static DigitalMediaObject givenDigitalMediaObject() throws JsonProcessingException {
    return givenDigitalMediaObject(DIGITAL_SPECIMEN_ID, PHYSICAL_SPECIMEN_ID, FORMAT, MEDIA_URL,
        TYPE);
  }

  public static DigitalMediaObject givenDigitalMediaObject(String digitalSpecimenId,
      String physicalSpecimenId, String format,
      String mediaUrl, String type) throws JsonProcessingException {
    return new DigitalMediaObject(
        type,
        digitalSpecimenId,
        physicalSpecimenId,
        generateAttributes(format, mediaUrl),
        generateOriginalAttributes()
    );
  }

  private static JsonNode generateAttributes(String format, String mediaUrl) {
    var objectNode = MAPPER.createObjectNode();
    objectNode.put("ac:accessURI", mediaUrl);
    objectNode.put("ods:sourceSystemId", "20.5000.1025/WDP-JYE-73C");
    objectNode.put("dcterms:format", format);
    objectNode.put("dcterms:license", "http://creativecommons.org/licenses/by-nc/3.0/");
    return objectNode;
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

}
