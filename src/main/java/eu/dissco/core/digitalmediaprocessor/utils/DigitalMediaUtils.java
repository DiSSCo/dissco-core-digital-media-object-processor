package eu.dissco.core.digitalmediaprocessor.utils;

import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.schema.DigitalMedia;
import java.util.Date;

public class DigitalMediaUtils {

  private DigitalMediaUtils() {
    // This is a utility class, so it should not be instantiated
  }

  public static DigitalMedia flattenToDigitalMedia(DigitalMediaRecord digitalMediaRecord){
    var digitalMedia = digitalMediaRecord.digitalMediaWrapper().attributes();
    digitalMedia.setId(digitalMediaRecord.id());
    digitalMedia.setOdsID(digitalMediaRecord.id());
    digitalMedia.setOdsVersion(digitalMediaRecord.version());
    digitalMedia.setOdsCreated(Date.from(digitalMediaRecord.created()));
    return digitalMedia;
  }
}
