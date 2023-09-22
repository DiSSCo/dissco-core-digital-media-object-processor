package eu.dissco.core.digitalmediaobjectprocessor.service;

import eu.dissco.core.digitalmediaobjectprocessor.schema.DigitalEntity;

public class ServiceUtils {

  private ServiceUtils() {
  }

  protected static String getMediaUrl(DigitalEntity attributes) {
    if (attributes.getAcAccessUri() != null) {
      return attributes.getAcAccessUri();
    }
    return null;
  }

}
