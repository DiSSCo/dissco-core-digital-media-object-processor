package eu.dissco.core.digitalmediaobjectprocessor.domain;

import com.fasterxml.jackson.databind.JsonNode;

public record DigitalMediaObjectTransfer(
    String type,
    String physicalSpecimenId,
    String mediaUrl,
    String format,
    String sourceSystemId,
    JsonNode data,
    JsonNode originalData,
    String idType) {

}
