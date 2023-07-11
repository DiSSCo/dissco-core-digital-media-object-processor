package eu.dissco.core.digitalmediaobjectprocessor.web;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectKey;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.PidAuthenticationException;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.PidCreationException;
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

  private static final String UNEXPECTED_MSG = "Unexpected response from handle API";
  private static final String UNEXPECTED_LOG = "Unexpected response from Handle API. Missing id and/or primarySpecimenObjectId. Response: {}";

  public Map<DigitalMediaObjectKey, String> postHandle(List<JsonNode> request)
      throws PidAuthenticationException, PidCreationException {
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.POST, requestBody, "batch");
    var responseJson = validateResponse(response);
    return parseResponse(responseJson);
  }

  public void rollbackHandleCreation(JsonNode request)
      throws PidCreationException, PidAuthenticationException {
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.DELETE, requestBody, "rollback");
    validateResponse(response);
  }

  public void rollbackHandleUpdate(List<JsonNode> request)
      throws PidCreationException, PidAuthenticationException {
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.DELETE, requestBody, "rollback/update");
    validateResponse(response);
  }

  private <T> Mono<JsonNode> sendRequest(HttpMethod httpMethod,
      BodyInserter<T, ReactiveHttpOutputMessage> requestBody, String endpoint)
      throws PidAuthenticationException {
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
                new PidAuthenticationException("Unable to authenticate with Handle Service.")))
        .onStatus(HttpStatusCode::is4xxClientError, r -> Mono.error(new PidCreationException(
            "Unable to create PID. Response from Handle API: " + r.statusCode())))
        .bodyToMono(JsonNode.class).retryWhen(
            Retry.fixedDelay(3, Duration.ofSeconds(2)).filter(WebClientUtils::is5xxServerError)
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> new PidCreationException(
                    "External Service failed to process after max retries")));
  }

  private JsonNode validateResponse (Mono<JsonNode> response) throws PidCreationException, PidAuthenticationException {
    try {
      return response.toFuture().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Interrupted exception has occurred.");
      throw new PidCreationException(
          "Interrupted execution: A connection error has occurred in creating a handle.");
    } catch (ExecutionException e) {
      if (e.getCause().getClass().equals(PidAuthenticationException.class)) {
        log.error(
            "Token obtained from Keycloak not accepted by Handle Server. Check Keycloak configuration.");
        throw new PidAuthenticationException(e.getCause().getMessage());
      }
      throw new PidCreationException(e.getCause().getMessage());
    }
  }

  private HashMap<DigitalMediaObjectKey, String> parseResponse(JsonNode handleResponse)
      throws PidCreationException {
    try {
      var dataNode = handleResponse.get("data");
      HashMap<DigitalMediaObjectKey, String> handleNames = new HashMap<>();
      if (!dataNode.isArray()) {
        log.error(UNEXPECTED_LOG, handleResponse.toPrettyString());
        throw new PidCreationException(UNEXPECTED_MSG);
      }
      for (var node : dataNode) {
        var handle = node.get("id");
        var primarySpecimenObjectId = node.get("attributes")
            .get("subjectLocalId").asText();
        var mediaUrl = node.get("attributes").get("mediaUrl").asText();
        DigitalMediaObjectKey key = new DigitalMediaObjectKey(primarySpecimenObjectId, mediaUrl);
        if (handle == null || primarySpecimenObjectId == null || mediaUrl == null) {
          log.error(UNEXPECTED_LOG, handleResponse.toPrettyString());
          throw new PidCreationException(UNEXPECTED_MSG);
        }
        handleNames.put(key, handle.asText());
      }
      return handleNames;
    } catch (NullPointerException e) {
      log.error(UNEXPECTED_LOG, handleResponse.toPrettyString());
      throw new PidCreationException(UNEXPECTED_MSG);
    }
  }

}
