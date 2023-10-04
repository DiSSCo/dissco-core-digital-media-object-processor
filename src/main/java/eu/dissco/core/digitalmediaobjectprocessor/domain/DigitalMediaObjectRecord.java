package eu.dissco.core.digitalmediaobjectprocessor.domain;

import java.time.Instant;

public record DigitalMediaObjectRecord(
    String id,
    int version,
    Instant created,
    DigitalMediaObjectWrapper digitalMediaObjectWrapper
) {

}
