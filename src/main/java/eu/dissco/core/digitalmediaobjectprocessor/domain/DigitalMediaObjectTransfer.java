package eu.dissco.core.digitalmediaobjectprocessor.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

public record DigitalMediaObjectTransfer(
    @JsonProperty("dcterms:type")
    String type,
    @JsonProperty("ods:physicalSpecimenId")
    String physicalSpecimenId,
    @JsonProperty("ods:attributes")
    JsonNode attributes,
    @JsonProperty("ods:originalAttributes")
    JsonNode originalAttributes) {

}
