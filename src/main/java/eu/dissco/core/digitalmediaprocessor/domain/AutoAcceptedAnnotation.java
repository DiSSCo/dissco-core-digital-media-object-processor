package eu.dissco.core.digitalmediaprocessor.domain;

import eu.dissco.core.digitalmediaprocessor.schema.Agent;
import eu.dissco.core.digitalmediaprocessor.schema.AnnotationProcessingRequest;
import java.util.List;

public record AutoAcceptedAnnotation(
    Agent acceptingAgent,
    List<AnnotationProcessingRequest> annotation
) {

}
