package eu.dissco.core.digitalmediaobjectprocessor.domain;

import java.util.List;

public record UpdatedDigitalMediaRecord(
    DigitalMediaObjectRecord digitalMediaObjectRecord,
    List<String> automatedAnnotations,
    DigitalMediaObjectRecord currentDigitalMediaRecord
) {

}
