package eu.dissco.core.digitalmediaobjectprocessor.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalmediaobjectprocessor.Profiles;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectEvent;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectRecord;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.DigitalSpecimenNotFoundException;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.NoChangesFoundException;
import eu.dissco.core.digitalmediaobjectprocessor.service.ProcessingService;
import javax.xml.transform.TransformerException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@Profile(Profiles.WEB)
@RestController
@RequestMapping("/")
@RequiredArgsConstructor
public class DigitalMediaObjectController {

  private final ProcessingService processingService;

  @PreAuthorize("isAuthenticated()")
  @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<DigitalMediaObjectRecord> createDigitalMediaObject(@RequestBody
  DigitalMediaObjectEvent event)
      throws JsonProcessingException, TransformerException, DigitalSpecimenNotFoundException, NoChangesFoundException {
    log.info("Received digitalMediaObject upsert: {}", event);
    var result = processingService.handleMessage(event, true);
    return ResponseEntity.status(HttpStatus.CREATED).body(result);
  }

  @ExceptionHandler(DigitalSpecimenNotFoundException.class)
  public ResponseEntity<String> handleException(DigitalSpecimenNotFoundException e) {
    return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(e.getMessage());
  }

  @ExceptionHandler(NoChangesFoundException.class)
  public ResponseEntity<String> handleException(NoChangesFoundException e) {
    return ResponseEntity.status(HttpStatus.OK).body(e.getMessage());
  }
}
