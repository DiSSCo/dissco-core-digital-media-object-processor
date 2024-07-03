package eu.dissco.core.digitalmediaprocessor.domain;

import java.util.List;

public record DigitalMediaEvent(List<String> enrichmentList,
                                DigitalMediaWrapper digitalMediaWrapper) {

}
