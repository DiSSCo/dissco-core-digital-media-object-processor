package eu.dissco.core.digitalmediaprocessor.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.properties.ElasticSearchProperties;
import java.io.IOException;
import java.util.Collection;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class ElasticSearchRepository {

  private final ElasticsearchClient client;
  private final ElasticSearchProperties properties;

  public BulkResponse indexDigitalMedia(
      Collection<DigitalMediaRecord> digitalMediaRecords) throws IOException {
    var bulkRequest = new BulkRequest.Builder();
    for (var digitalMediaRecord : digitalMediaRecords) {
      bulkRequest.operations(op ->
          op.index(idx ->
              idx.index(properties.getIndexName())
                  .id(digitalMediaRecord.id())
                  .document(digitalMediaRecord))
      );
    }
    return client.bulk(bulkRequest.build());
  }

  public DeleteResponse rollbackDigitalMedia(DigitalMediaRecord digitalMediaRecord)
      throws IOException {
    return client.delete(d -> d.index(properties.getIndexName()).id(digitalMediaRecord.id()));
  }

  public void rollbackVersion(DigitalMediaRecord currentDigitalMediaRecord)
      throws IOException {
    client.index(i -> i.index(properties.getIndexName()).id(currentDigitalMediaRecord.id())
        .document(currentDigitalMediaRecord));
  }
}
