package eu.dissco.core.digitalmediaobjectprocessor.domain;

import java.util.List;

public record DigitalMediaObjectEvent(List<String> enrichmentList,
                                      DigitalMediaObject digitalMediaObject) {

}
