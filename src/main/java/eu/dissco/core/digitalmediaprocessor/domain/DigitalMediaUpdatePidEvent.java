package eu.dissco.core.digitalmediaprocessor.domain;

public record DigitalMediaUpdatePidEvent(
    String digitalSpecimenPID,
    String digitalMediaPID,
    String digitalMediaAccessURI) {
}
