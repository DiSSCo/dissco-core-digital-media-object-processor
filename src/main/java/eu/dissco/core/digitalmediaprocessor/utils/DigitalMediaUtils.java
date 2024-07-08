package eu.dissco.core.digitalmediaprocessor.utils;

import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.schema.DigitalMedia;
import java.util.Date;

public class DigitalMediaUtils {

  public static String DOI_PREFIX = "https://doi.org/";

  private DigitalMediaUtils() {
    // This is a utility class, so it should not be instantiated
  }

  public static DigitalMedia flattenToDigitalMedia(DigitalMediaRecord digitalMediaRecord) {
    var digitalMedia = digitalMediaRecord.digitalMediaWrapper().attributes();
    digitalMedia.setId(DOI_PREFIX + digitalMediaRecord.id());
    digitalMedia.setOdsID(DOI_PREFIX + digitalMediaRecord.id());
    digitalMedia.setOdsVersion(digitalMediaRecord.version());
    digitalMedia.setDctermsCreated(Date.from(digitalMediaRecord.created()));
    return digitalMedia;
  }
}
