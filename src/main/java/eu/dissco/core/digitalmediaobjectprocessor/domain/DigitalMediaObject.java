package eu.dissco.core.digitalmediaobjectprocessor.domain;

import com.fasterxml.jackson.databind.JsonNode;

public record DigitalMediaObject(
    String type,
    String digitalSpecimenId,
    String mediaUrl,
    String format,
    String sourceSystemId,
    JsonNode data,
    JsonNode originalData) {

}
