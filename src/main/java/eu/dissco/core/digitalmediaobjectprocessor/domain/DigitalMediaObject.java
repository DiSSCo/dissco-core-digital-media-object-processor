package eu.dissco.core.digitalmediaobjectprocessor.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalmediaobjectprocessor.schema.DigitalEntity;

public record DigitalMediaObject(
    @JsonProperty("dcterms:type")
    String type,
    @JsonProperty("ods:digitalSpecimenId")
    String digitalSpecimenId,
    @JsonProperty("ods:attributes")
    DigitalEntity attributes,
    @JsonProperty("ods:originalAttributes")
    JsonNode originalAttributes) {

}
