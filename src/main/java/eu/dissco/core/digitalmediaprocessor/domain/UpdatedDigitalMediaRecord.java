package eu.dissco.core.digitalmediaprocessor.domain;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

public record UpdatedDigitalMediaRecord(
    DigitalMediaRecord digitalMediaRecord,
    List<String> automatedAnnotations,
    DigitalMediaRecord currentDigitalMediaRecord,
    JsonNode jsonPatch
) {

}
