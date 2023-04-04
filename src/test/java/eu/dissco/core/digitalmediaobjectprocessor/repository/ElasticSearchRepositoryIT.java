package eu.dissco.core.digitalmediaobjectprocessor.repository;

import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.DIGITAL_SPECIMEN_ID_2;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.DIGITAL_SPECIMEN_ID_3;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.HANDLE;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.HANDLE_2;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.HANDLE_3;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MAPPER;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MEDIA_URL_2;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.MEDIA_URL_3;
import static eu.dissco.core.digitalmediaobjectprocessor.TestUtils.givenDigitalMediaObjectRecord;
import static org.assertj.core.api.Assertions.assertThat;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectRecord;
import eu.dissco.core.digitalmediaobjectprocessor.properties.ElasticSearchProperties;
import java.io.IOException;
import java.util.List;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class ElasticSearchRepositoryIT {

  private static final DockerImageName ELASTIC_IMAGE = DockerImageName.parse(
      "docker.elastic.co/elasticsearch/elasticsearch").withTag("8.6.1");
  private static final String INDEX = "digital-media-object";
  private static final String ELASTICSEARCH_USERNAME = "elastic";
  private static final String ELASTICSEARCH_PASSWORD = "s3cret";
  private static final ElasticsearchContainer container = new ElasticsearchContainer(
      ELASTIC_IMAGE).withPassword(ELASTICSEARCH_PASSWORD);
  private static ElasticsearchClient client;
  private static RestClient restClient;
  private final ElasticSearchProperties esProperties = new ElasticSearchProperties();
  private ElasticSearchRepository repository;

  @BeforeAll
  static void initContainer() {
    // Create the elasticsearch container.
    container.start();

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD));

    HttpHost host = new HttpHost("localhost",
        container.getMappedPort(9200), "https");
    final RestClientBuilder builder = RestClient.builder(host);

    builder.setHttpClientConfigCallback(clientBuilder -> {
      clientBuilder.setSSLContext(container.createSslContextFromCa());
      clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
      return clientBuilder;
    });
    restClient = builder.build();

    ElasticsearchTransport transport = new RestClientTransport(restClient,
        new JacksonJsonpMapper(MAPPER));

    client = new ElasticsearchClient(transport);
  }

  @AfterAll
  public static void closeResources() throws Exception {
    restClient.close();
  }

  @BeforeEach
  void initRepository() {
    repository = new ElasticSearchRepository(client, esProperties);
  }

  @AfterEach
  void clearIndex() throws IOException {
    client.indices().delete(b -> b.index(INDEX));
  }

  @Test
  void testIndexDigitalMediaObject() throws IOException {
    // Given
    esProperties.setIndexName(INDEX);

    // When
    var result = repository.indexDigitalMediaObject(List.of(
        givenDigitalMediaObjectRecord(),
        givenDigitalMediaObjectRecord(HANDLE_2, DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2)));

    // Then

    var document = client.get(g -> g.index(INDEX).id(HANDLE),
        DigitalMediaObjectRecord.class);
    assertThat(result.errors()).isFalse();
    assertThat(document.source()).isEqualTo(givenDigitalMediaObjectRecord());
    assertThat(result.items().get(0).result()).isEqualTo("created");
  }

  @Test
  void testRollbackSpecimen() throws IOException {
    // Given
    esProperties.setIndexName(INDEX);
    repository.indexDigitalMediaObject(List.of(
        givenDigitalMediaObjectRecord(),
        givenDigitalMediaObjectRecord(HANDLE_2, DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2),
        givenDigitalMediaObjectRecord(HANDLE_3, DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3)));

    // When
    var result = repository.rollbackSpecimen(givenDigitalMediaObjectRecord());

    // Then
    var document = client.get(g -> g.index(INDEX).id(HANDLE),
        DigitalMediaObjectRecord.class);
    assertThat(document.source()).isNull();
    assertThat(document.found()).isFalse();
    assertThat(result.result()).isEqualTo(Result.Deleted);
  }

  @Test
  void testRollbackVersion() throws IOException {
    // Given
    esProperties.setIndexName(INDEX);
    repository.indexDigitalMediaObject(List.of(
        givenDigitalMediaObjectRecord(),
        givenDigitalMediaObjectRecord(HANDLE_2, DIGITAL_SPECIMEN_ID_2, MEDIA_URL_2),
        givenDigitalMediaObjectRecord(HANDLE_3, DIGITAL_SPECIMEN_ID_3, MEDIA_URL_3)));

    // When
    repository.rollbackVersion(givenDigitalMediaObjectRecord("image/png"));

    // Then
    var document = client.get(g -> g.index(INDEX).id(HANDLE),
        DigitalMediaObjectRecord.class);
    assertThat(document.source().digitalMediaObject().attributes().get("dcterms:format")
        .asText()).isEqualTo("image/png");
  }


}

