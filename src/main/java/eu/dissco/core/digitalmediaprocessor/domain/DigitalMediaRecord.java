package eu.dissco.core.digitalmediaprocessor.domain;

import java.time.Instant;

public record DigitalMediaRecord(
    String id,
    int version,
    Instant created,
    DigitalMediaWrapper digitalMediaWrapper
) {

}
