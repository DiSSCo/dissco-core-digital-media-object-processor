package eu.dissco.core.digitalmediaobjectprocessor.web;

import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.DIGITAL_SPECIMEN_ID;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MAPPER;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MEDIA_URL_1;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenPidMap;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
    var responseBody = givenHandleRequest();
    var expected = givenPidMap(1);
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(List.of(responseBody)))
        .addHeader("Content-Type", "application/json"));

    // When
    var response = handleComponent.postHandle(requestBody);

    // Then
    assertThat(response).isEqualTo(expected);
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
              "subjectIdentifier":"20.5000.1025/460-A7R-QMJ"
              }
           }
        }
        """);
  }


}
