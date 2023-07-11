package eu.dissco.core.digitalmediaobjectprocessor.web;

import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.DIGITAL_SPECIMEN_ID;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MAPPER;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MEDIA_URL_1;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenPidMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.PidAuthenticationException;
import eu.dissco.core.digitalmediaobjectprocessor.exceptions.PidCreationException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClient;

@ExtendWith(MockitoExtension.class)
class HandleComponentTest {

  @Mock
  private TokenAuthenticator tokenAuthenticator;
  private HandleComponent handleComponent;

  private static MockWebServer mockHandleServer;

  @BeforeAll
  static void init() throws IOException {
    mockHandleServer = new MockWebServer();
    mockHandleServer.start();
  }

  @BeforeEach
  void setup() {
    WebClient webClient = WebClient.create(
        String.format("http://%s:%s", mockHandleServer.getHostName(), mockHandleServer.getPort()));
    handleComponent = new HandleComponent(webClient, tokenAuthenticator);

  }

  @AfterAll
  static void destroy() throws IOException {
    mockHandleServer.shutdown();
  }

  @Test
  void testPostHandle() throws Exception {
    // Given
    var requestBody = List.of(givenHandleRequest());
    var responseBody = givenHandleResponse();
    var expected = givenPidMap(1);
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));

    // When
    var response = handleComponent.postHandle(requestBody);

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @Test
  void testUnauthorized() throws Exception {
    // Given
    var requestBody = List.of(givenHandleRequest());

    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.UNAUTHORIZED.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertThrows(PidAuthenticationException.class, () -> handleComponent.postHandle(requestBody));
  }

  @Test
  void testBadRequest() throws Exception {
    // Given
    var requestBody = List.of(givenHandleRequest());

    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.BAD_REQUEST.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
  }

  @Test
  void testRetriesSuccess() throws Exception {
    // Given
    var requestBody = List.of(givenHandleRequest());
    var responseBody = givenHandleResponse();
    var expected = givenPidMap(1);
    int requestCount = mockHandleServer.getRequestCount();

    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));

    // When
    var response = handleComponent.postHandle(requestBody);

    // Then
    assertThat(response).isEqualTo(expected);
    assertThat(mockHandleServer.getRequestCount() - requestCount).isEqualTo(2);
  }

  @Test
  void testRollbackHandleCreation() throws Exception {
    // Given
    var requestBody = MAPPER.readTree("""
        {
          "data": [
            {"id": "20.5000.1025/AAA-111-AAA"},
            {"id": "20.5000.1025/BBB-222-BBB"}
          ]
        }
        """);
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertDoesNotThrow(() -> handleComponent.rollbackHandleCreation(requestBody));
  }

  @Test
  void testRollbackHandleUpdate() throws Exception {
    // Given
    var requestBody = givenHandleRequest();
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertDoesNotThrow(() -> handleComponent.rollbackHandleUpdate(List.of(requestBody)));
  }

  @Test
  void testInterruptedException() throws Exception {
    // Given
    var requestBody = givenHandleRequest();
    var responseBody = givenHandleResponse();

    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));

    Thread.currentThread().interrupt();

    // When
    var response = assertThrows(PidCreationException.class,
        () -> handleComponent.postHandle(List.of(requestBody)));

    // Then
    assertThat(response).hasMessage(
        "Interrupted execution: A connection error has occurred in creating a handle.");
  }

  @Test
  void testRetriesFail() throws Exception {
    // Given
    var requestBody = List.of(givenHandleRequest());
    int requestCount = mockHandleServer.getRequestCount();

    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));
    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));
    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));
    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));

    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
    assertThat(mockHandleServer.getRequestCount() - requestCount).isEqualTo(4);
  }

  @Test
  void testDataNodeNotArray() throws Exception {
    // Given
    var requestBody = List.of(givenHandleRequest());
    var responseBody = MAPPER.createObjectNode();
    responseBody.put("data", "val");
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));
    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
  }

  @Test
  void testDataMissingId() throws Exception {
    // Given
    var requestBody = List.of(givenHandleRequest());
    var responseBody = givenHandleRequest();

    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));
    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
  }

  @ParameterizedTest
  @ValueSource(strings = {"subjectLocalId", "mediaUrl"})
  void testMissingDigitalMediaKey(String attribute) throws Exception {
    // Given
    var requestBody = List.of(givenHandleRequest());
    var responseBody = removeGivenAttribute(attribute);
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));

    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
  }

  @Test
  void testEmptyResponse() throws Exception {
    // Given
    var requestBody = List.of(givenHandleRequest());
    var responseBody = MAPPER.createObjectNode();

    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));
    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
  }

  private JsonNode givenHandleRequest() throws Exception{
    return MAPPER.readTree("""
        {
        "data": {
            "type": "mediaObject",
            "attributes": {
              "fdoProfile": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
              "digitalObjectType": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
              "issuedForAgent": "https://ror.org/0566bfb96",
              "mediaHash":"",
              "mediaHashAlgorithm":"",
              "subjectSpecimenHost":"",
              "mediaUrl":"http://data.rbge.org.uk/living/19942272",
              "subjectLocalId":"20.5000.1025/460-A7R-QMJ"
              }
           }
        }
        """);
  }

  private JsonNode removeGivenAttribute(String targetAttribute) throws Exception {
    var response = (ObjectNode) givenHandleResponse();
    return ((ObjectNode) response.get("data").get(0).get("attributes")).remove(targetAttribute);
  }

  private JsonNode givenHandleResponse() throws Exception{
    return MAPPER.readTree("""
        {
        "data": [{
            "type": "mediaObject",
            "id":"20.5000.1025/1BY-BHB-AVN",
            "attributes": {
              "fdoProfile": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
              "digitalObjectType": "https://hdl.handle.net/21.T11148/64396cf36b976ad08267",
              "issuedForAgent": "https://ror.org/0566bfb96",
              "mediaHash":"",
              "mediaHashAlgorithm":"",
              "subjectSpecimenHost":"",
              "mediaUrl":"http://data.rbge.org.uk/living/19942272",
              "subjectLocalId":"20.5000.1025/460-A7R-QMJ"
              }
           }]
        }
        """);
  }


}
