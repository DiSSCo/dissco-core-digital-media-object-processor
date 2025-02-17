package eu.dissco.core.digitalmediaprocessor.domain;

import eu.dissco.core.digitalmediaprocessor.schema.Agent;
import eu.dissco.core.digitalmediaprocessor.schema.AnnotationProcessingRequest;

public record AutoAcceptedAnnotation(
    Agent acceptingAgent,
    AnnotationProcessingRequest annotation
) {

}
