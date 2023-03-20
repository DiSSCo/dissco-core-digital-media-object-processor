package eu.dissco.core.digitalmediaobjectprocessor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObject;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectEvent;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectRecord;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectTransfer;
import java.time.Instant;
import java.util.List;

public class TestUtils {

  public static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

  public static final String ID = "20.5000.1025/1BY-BHB-AVN";
  public static final int VERSION = 1;
  public static final Instant CREATED = Instant.parse("2022-09-16T08:52:27.391Z");
  public static final String TYPE = "2DImageObject";
  public static final String AAS = "OCR";
  public static final String PHYSICAL_SPECIMEN_ID = "045db6cb-5f06-4c19-b0f6-9620bdff3ae4:040ck2b86";
  public static final String DIGITAL_SPECIMEN_ID = "20.5000.1025/460-A7R-QMJ";
  public static final String MEDIA_URL = "http://data.rbge.org.uk/living/19942272";
  public static final String FORMAT = "image/jpeg";

  public static DigitalMediaObjectEvent givenMediaEvent() throws JsonProcessingException {
    return new DigitalMediaObjectEvent(
        List.of("OCR"), givenDigitelMediaTransfer()
    );
  }

  private static DigitalMediaObjectTransfer givenDigitelMediaTransfer()
      throws JsonProcessingException {
    return new DigitalMediaObjectTransfer(
        TYPE,
        PHYSICAL_SPECIMEN_ID,
        "combined",
        generateAttributes(),
        generateOriginalAttributes()
    );
  }

  public static DigitalMediaObjectRecord givenDigitalMediaObjectRecord()
      throws JsonProcessingException {
    return givenDigitalMediaObjectRecord(FORMAT);
  }

  public static DigitalMediaObjectRecord givenDigitalMediaObjectRecord(String format)
      throws JsonProcessingException {
    return new DigitalMediaObjectRecord(
        ID,
        VERSION,
        CREATED,
        givenDigitalMediaObject(format)
        );
  }

  public static DigitalMediaObject givenDigitalMediaObject() throws JsonProcessingException {
    return givenDigitalMediaObject(FORMAT);
  }

  public static DigitalMediaObject givenDigitalMediaObject(String format) throws JsonProcessingException {
    return new DigitalMediaObject(
        TYPE,
        DIGITAL_SPECIMEN_ID,
        generateAttributes(),
        generateOriginalAttributes()
    );
  }

  private static JsonNode generateAttributes() throws JsonProcessingException {
    return MAPPER.readValue(
        """
            {
              "ac:accessURI": "http://data.rbge.org.uk/living/19942272",
              "ods:sourceSystemId": "20.5000.1025/WDP-JYE-73C",
              "dcterms:format": "image/jpeg",
              "dcterms:license": "http://creativecommons.org/licenses/by-nc/3.0/"
              }
            """, JsonNode.class
    );
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
