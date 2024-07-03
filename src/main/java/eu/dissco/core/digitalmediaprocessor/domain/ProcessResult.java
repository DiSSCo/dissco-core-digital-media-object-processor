package eu.dissco.core.digitalmediaprocessor.domain;

import java.util.List;

public record ProcessResult(
    List<DigitalMediaRecord> equalDigitalMedia,
    List<UpdatedDigitalMediaTuple> changedDigitalMedia,
    List<DigitalMediaEvent> newDigitalMedia
) {

}