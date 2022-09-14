package eu.dissco.core.digitalmediaobjectprocessor.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectRecord;
import eu.dissco.core.digitalmediaobjectprocessor.properties.ElasticSearchProperties;
import java.io.IOException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class ElasticSearchRepository {

  private final ElasticsearchClient client;
  private final ElasticSearchProperties properties;

  public IndexResponse indexDigitalMediaObject(DigitalMediaObjectRecord digitalMediaObjectRecord) {
    try {
      return client.index(
          idx -> idx.index(properties.getIndexName()).id(digitalMediaObjectRecord.id())
              .document(digitalMediaObjectRecord));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
