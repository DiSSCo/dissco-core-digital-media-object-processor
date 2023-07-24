package eu.dissco.core.digitalmediaobjectprocessor.service;

import com.fasterxml.jackson.databind.JsonNode;

public class ServiceUtils {

  private ServiceUtils(){}

  protected static String getMediaUrl(JsonNode attributes) {
    if (attributes.get("ac:accessURI") != null) {
      return attributes.get("ac:accessURI").asText();
    }
    return null;
  }

}
