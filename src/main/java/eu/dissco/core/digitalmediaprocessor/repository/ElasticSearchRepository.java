package eu.dissco.core.digitalmediaprocessor.repository;

import static eu.dissco.core.digitalmediaprocessor.utils.DigitalMediaUtils.DOI_PREFIX;
import static eu.dissco.core.digitalmediaprocessor.utils.DigitalMediaUtils.flattenToDigitalMedia;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.properties.ElasticSearchProperties;
import eu.dissco.core.digitalmediaprocessor.utils.DigitalMediaUtils;
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
      var digitalMedia = DigitalMediaUtils.flattenToDigitalMedia(digitalMediaRecord);
      bulkRequest.operations(op ->
          op.index(idx ->
              idx.index(properties.getIndexName())
                  .id(digitalMedia.getId())
                  .document(digitalMedia))
      );
    }
    return client.bulk(bulkRequest.build());
  }

  public DeleteResponse rollbackDigitalMedia(DigitalMediaRecord digitalMediaRecord)
      throws IOException {
    return client.delete(
        d -> d.index(properties.getIndexName()).id(DOI_PREFIX + digitalMediaRecord.id()));
  }

  public void rollbackVersion(DigitalMediaRecord currentDigitalMediaRecord)
      throws IOException {
    var digitalMedia = flattenToDigitalMedia(currentDigitalMediaRecord);
    client.index(i -> i.index(properties.getIndexName()).id(digitalMedia.getId())
        .document(digitalMedia));
  }
}
