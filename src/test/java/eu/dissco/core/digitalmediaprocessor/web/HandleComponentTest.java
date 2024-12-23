package eu.dissco.core.digitalmediaprocessor.web;

import static eu.dissco.core.digitalmediaprocessor.TestUtils.CREATED;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.HANDLE;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.HANDLE_2;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.MAPPER;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenPidMap;
import static eu.dissco.core.digitalmediaprocessor.TestUtils.givenPostHandleRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mockStatic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.core.digitalmediaprocessor.exceptions.PidCreationException;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClient;

@ExtendWith(MockitoExtension.class)
class HandleComponentTest {

  private static MockWebServer mockHandleServer;
  private final Instant instant = Instant.now(Clock.fixed(CREATED, ZoneOffset.UTC));
  @Mock
  private TokenAuthenticator tokenAuthenticator;
  private HandleComponent handleComponent;
  private MockedStatic<Clock> mockedClock;
  private MockedStatic<Instant> mockedStatic;

  @BeforeAll
  static void init() throws IOException {
    mockHandleServer = new MockWebServer();
    mockHandleServer.start();
  }

  @AfterAll
  static void destroy() throws IOException {
    mockHandleServer.shutdown();
  }

  @BeforeEach
  void setup() {
    WebClient webClient = WebClient.create(
        String.format("http://%s:%s", mockHandleServer.getHostName(), mockHandleServer.getPort()));
    handleComponent = new HandleComponent(webClient, tokenAuthenticator, MAPPER);

    Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
    mockedStatic = mockStatic(Instant.class);
    mockedStatic.when(Instant::now).thenReturn(instant);
    mockedClock = mockStatic(Clock.class);
    mockedClock.when(Clock::systemUTC).thenReturn(clock);
  }

  @AfterEach
  void takeDown() {
    mockedStatic.close();
    mockedClock.close();
  }

  @Test
  void testPostHandle() throws Exception {
    // Given
    var requestBody = List.of(givenPostHandleRequest());
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
  void testUnauthorized() {
    // Given
    var requestBody = List.of(givenPostHandleRequest());

    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.UNAUTHORIZED.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
  }

  @Test
  void testBadRequest() {
    // Given
    var requestBody = List.of(givenPostHandleRequest());

    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.BAD_REQUEST.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
  }

  @Test
  void testRetriesSuccess() throws Exception {
    // Given
    var requestBody = List.of(givenPostHandleRequest());
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
  void testRollbackHandleCreation() {
    // Given
    var requestBody = List.of(HANDLE, HANDLE_2);
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertDoesNotThrow(() -> handleComponent.rollbackHandleCreation(requestBody));
  }

  @Test
  void testRollbackHandleUpdate() {
    // Given
    var requestBody = givenPostHandleRequest();
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertDoesNotThrow(() -> handleComponent.rollbackHandleUpdate(List.of(requestBody)));
  }

  @Test
  void testUpdateHandle() {
    // Given
    var requestBody = givenPostHandleRequest();
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertDoesNotThrow(() -> handleComponent.updateHandle(List.of(requestBody)));
  }

  @Test
  void testInterruptedException() throws Exception {
    // Given
    var requestBody = givenPostHandleRequest();
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
  void testRetriesFail() {
    // Given
    var requestBody = List.of(givenPostHandleRequest());
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
    var requestBody = List.of(givenPostHandleRequest());
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
    var requestBody = List.of(givenPostHandleRequest());
    var responseBody = givenPostHandleRequest();

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
    var requestBody = List.of(givenPostHandleRequest());
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
    var requestBody = List.of(givenPostHandleRequest());
    var responseBody = MAPPER.createObjectNode();

    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));
    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
  }


  @Test
  void testActivatePidsEmpty() {
    // When / Then
    assertDoesNotThrow(() -> handleComponent.activatePids(List.of()));
  }

  @Test
  void testActivatePids() {
    // When / Then
    assertDoesNotThrow(() -> handleComponent.activatePids(List.of(HANDLE)));
  }

  private static JsonNode removeGivenAttribute(String targetAttribute) throws Exception {
    var response = (ObjectNode) givenHandleResponse();
    return ((ObjectNode) response.get("data").get(0).get("attributes")).remove(targetAttribute);
  }

  private static JsonNode givenHandleResponse() throws Exception {
    return MAPPER.readTree("""
        {
        "data": [{
            "type": "mediaObject",
            "id":"20.5000.1025/1BY-BHB-AVN",
            "attributes": {
              "digitalMediaKey":{
                "mediaUrl":"http://data.rbge.org.uk/living/19942272",
                "digitalSpecimenId":"20.5000.1025/460-A7R-QMJ"
                }
              }
           }]
        }
        """);
  }

}
