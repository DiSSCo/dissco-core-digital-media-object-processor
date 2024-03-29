package eu.dissco.core.digitalmediaobjectprocessor.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectRecord;
import eu.dissco.core.digitalmediaobjectprocessor.properties.ElasticSearchProperties;
import java.io.IOException;
import java.util.Collection;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class ElasticSearchRepository {

  private final ElasticsearchClient client;
  private final ElasticSearchProperties properties;

  public BulkResponse indexDigitalMediaObject(
      Collection<DigitalMediaObjectRecord> digitalMediaObjectRecords) throws IOException {
    var bulkRequest = new BulkRequest.Builder();
    for (var digitalMediaObjectRecord : digitalMediaObjectRecords) {
      bulkRequest.operations(op ->
          op.index(idx ->
              idx.index(properties.getIndexName())
                  .id(digitalMediaObjectRecord.id())
                  .document(digitalMediaObjectRecord))
      );
    }
    return client.bulk(bulkRequest.build());
  }

  public DeleteResponse rollbackDigitalMedia(DigitalMediaObjectRecord digitalMediaRecord)
      throws IOException {
    return client.delete(d -> d.index(properties.getIndexName()).id(digitalMediaRecord.id()));
  }

  public void rollbackVersion(DigitalMediaObjectRecord currentDigitalMediaRecord)
      throws IOException {
    client.index(i -> i.index(properties.getIndexName()).id(currentDigitalMediaRecord.id())
        .document(currentDigitalMediaRecord));
  }
}
