package eu.dissco.core.digitalmediaobjectprocessor.domain;

import java.util.List;

public record ProcessResult(
    List<DigitalMediaObjectRecord> equalMediaObjects,
    List<UpdatedDigitalMediaTuple> changedMediaObjects,
    List<DigitalMediaObjectEvent> newMediaObjects
) {

}