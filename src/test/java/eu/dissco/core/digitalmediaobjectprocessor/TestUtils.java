package eu.dissco.core.digitalmediaobjectprocessor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObject;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectRecord;
import java.time.Instant;

public class TestUtils {

  public static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

  public static final String ID = "20.5000.1025/1BY-BHB-AVN";
  public static final int VERSION = 1;
  public static final Instant CREATED = Instant.parse("2022-09-16T08:52:27.391Z");
  public static final String TYPE = "2DImageObject";
  public static final String DIGITAL_SPECIMEN_ID = "20.5000.1025/460-A7R-QMJ";
  public static final String MEDIA_URL = "http://repo.rbge.org.uk/image_server.php?kind=1500&path_base64=L2l0ZW1faW1hZ2VzL2FjY2Vzc2lvbnMvMTkvOTQvMjIvNzIvUGhvdG9fNTFjMWNlNzM5ZDE2Zi5qcGc=";
  public static final String FORMAT = "image/jpeg";
  public static final String SOURCE_SYSTEM_ID = "20.5000.1025/GW0-TYL-YRU";

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
        new DigitalMediaObject(
            TYPE,
            DIGITAL_SPECIMEN_ID,
            MEDIA_URL,
            format,
            SOURCE_SYSTEM_ID,
            generateAttributes(),
            generateOriginalAttributes()
        )
    );
  }

  private static JsonNode generateAttributes() throws JsonProcessingException {
    return MAPPER.readValue(
        """
            {
              "dcterms:title": "19942272",
              "dcterms:creator": "Unknown",
              "dcterms:license": "http://creativecommons.org/licenses/by-nc/3.0/",
              "dcterms:publisher": "Royal Botanic Garden Edinburgh",
              "dcterms:references": "http://data.rbge.org.uk/living/19942272",
              "dcterms:description": "Image of living collection specimen with accession number 19942272 by Unknown",
              "dcterms:rightsHolder": "Copyright Royal Botanic Garden Edinburgh. Contact us for rights to commercial use."
            }
            """, JsonNode.class
    );
  }

  private static JsonNode generateOriginalAttributes() throws JsonProcessingException {
    return MAPPER.readValue(
        """
             {
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
