package eu.dissco.core.digitalmediaprocessor.web;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaKey;
import eu.dissco.core.digitalmediaprocessor.exceptions.PidCreationException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ReactiveHttpOutputMessage;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserter;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Component
@RequiredArgsConstructor
@Slf4j
public class HandleComponent {

  @Qualifier("handleClient")
  private final WebClient handleClient;
  private final TokenAuthenticator tokenAuthenticator;
  private final ObjectMapper mapper;

  private static final String UNEXPECTED_MSG = "Unexpected response from handle API";
  private static final String UNEXPECTED_LOG = "Unexpected response from Handle API. Missing id and/or primarySpecimenObjectId. Response: {}";

  public Map<DigitalMediaKey, String> postHandle(List<JsonNode> request)
      throws PidCreationException {
    log.info("Posting {} Digital Media Objects to Handle API", request.size());
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.POST, requestBody, "batch");
    var responseJson = validateResponse(response);
    return parseResponse(responseJson);
  }

  public void activatePids(List<String> handles) {
    if (handles.isEmpty()) {
      return;
    }
    log.info("Activating {} handles", handles.size());
    var requestBody = BodyInserters.fromValue(handles);
    try {
      sendRequest(HttpMethod.POST, requestBody, "activate");
    } catch (PidCreationException e){
      log.error("Unable to activate handles. Manually activate the following handles {}", handles, e);
    }
  }

  public void updateHandle(List<JsonNode> request) throws PidCreationException {
    log.info("Updating {} Digital Media Object Handles", request.size());
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.PATCH, requestBody, "");
    validateResponse(response);
  }

  public void rollbackHandleCreation(List<String> request)
      throws PidCreationException {
    log.info("Rolling back handle creation");
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.DELETE, requestBody, "rollback/create");
    validateResponse(response);
  }

  public void rollbackHandleUpdate(List<JsonNode> request)
      throws PidCreationException {
    log.info("Rolling back handle update for {} handles", request.size());
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.DELETE, requestBody, "rollback/update");
    validateResponse(response);
  }

  private <T> Mono<JsonNode> sendRequest(HttpMethod httpMethod,
      BodyInserter<T, ReactiveHttpOutputMessage> requestBody, String endpoint)
      throws PidCreationException {
    var token = "Bearer " + tokenAuthenticator.getToken();
    return handleClient
        .method(httpMethod)
        .uri(uriBuilder -> uriBuilder.path(endpoint).build())
        .body(requestBody)
        .header("Authorization", token)
        .acceptCharset(StandardCharsets.UTF_8)
        .retrieve()
        .onStatus(HttpStatus.UNAUTHORIZED::equals,
            r -> Mono.error(
                new PidCreationException("Unable to authenticate with Handle Service.")))
        .onStatus(HttpStatusCode::is4xxClientError, r -> Mono.error(new PidCreationException(
            "Unable to create PID. Response from Handle API: " + r.statusCode())))
        .bodyToMono(JsonNode.class).retryWhen(
            Retry.fixedDelay(3, Duration.ofSeconds(2)).filter(WebClientUtils::is5xxServerError)
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> new PidCreationException(
                    "External Service failed to process after max retries")));
  }

  private JsonNode validateResponse(Mono<JsonNode> response) throws PidCreationException {
    try {
      return response.toFuture().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Interrupted exception has occurred.");
      throw new PidCreationException(
          "Interrupted execution: A connection error has occurred in creating a handle.");
    } catch (ExecutionException e) {
      log.error("PID creation failed.", e.getCause());
      throw new PidCreationException(e.getCause().getMessage());
    }
  }

  private HashMap<DigitalMediaKey, String> parseResponse(JsonNode handleResponse)
      throws PidCreationException {
    try {
      var dataNode = handleResponse.get("data");
      HashMap<DigitalMediaKey, String> handleNames = new HashMap<>();
      if (!dataNode.isArray()) {
        log.error(UNEXPECTED_LOG, handleResponse.toPrettyString());
        throw new PidCreationException(UNEXPECTED_MSG);
      }
      for (var node : dataNode) {
        var handle = node.get("id");
        var key = mapper.treeToValue(node.get("attributes").get("digitalMediaKey"), DigitalMediaKey.class);
        handleNames.put(key, handle.asText());
      }
      return handleNames;
    } catch (NullPointerException | JsonProcessingException e) {
      log.error(UNEXPECTED_LOG, handleResponse.toPrettyString(), e);
      throw new PidCreationException(UNEXPECTED_MSG);
    }
  }

}
