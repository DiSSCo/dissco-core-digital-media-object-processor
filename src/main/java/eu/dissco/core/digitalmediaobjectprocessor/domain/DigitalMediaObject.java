package eu.dissco.core.digitalmediaobjectprocessor.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

public record DigitalMediaObject(
    @JsonProperty("dcterms:type")
    String type,
    @JsonProperty("ods:digitalSpecimenId")
    String digitalSpecimenId,
    @JsonProperty("ods:attributes")
    JsonNode attributes,
    @JsonProperty("ods:originalAttributes")
    JsonNode originalAttributes) {

}
