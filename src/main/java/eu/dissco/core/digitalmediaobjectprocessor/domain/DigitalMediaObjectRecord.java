package eu.dissco.core.digitalmediaobjectprocessor.domain;

public record DigitalMediaObjectRecord(
    String id,
    int version,
    DigitalMediaObject digitalMediaObject
) {

}
