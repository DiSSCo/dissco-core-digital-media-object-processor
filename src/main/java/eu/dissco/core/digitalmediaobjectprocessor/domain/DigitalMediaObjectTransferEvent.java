package eu.dissco.core.digitalmediaobjectprocessor.domain;

import java.util.List;

public record DigitalMediaObjectTransferEvent(
    List<String> enrichmentList,
    DigitalMediaObjectTransfer digitalMediaObject) {

}
