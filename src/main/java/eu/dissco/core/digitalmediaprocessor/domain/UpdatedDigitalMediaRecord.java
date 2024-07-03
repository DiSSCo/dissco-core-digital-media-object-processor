package eu.dissco.core.digitalmediaprocessor.domain;

import java.util.List;

public record UpdatedDigitalMediaRecord(
    DigitalMediaRecord digitalMediaRecord,
    List<String> automatedAnnotations,
    DigitalMediaRecord currentDigitalMediaRecord
) {

}
