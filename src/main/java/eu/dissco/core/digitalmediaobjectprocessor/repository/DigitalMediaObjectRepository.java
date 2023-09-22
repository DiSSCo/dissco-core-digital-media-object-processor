package eu.dissco.core.digitalmediaobjectprocessor.repository;

import static eu.dissco.core.digitalmediaobjectprocessor.database.jooq.Tables.DIGITAL_MEDIA_OBJECT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObject;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectRecord;
import eu.dissco.core.digitalmediaobjectprocessor.schema.DigitalEntity;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Query;
import org.jooq.Record;
import org.springframework.stereotype.Repository;

@Slf4j
@Repository
@RequiredArgsConstructor
public class DigitalMediaObjectRepository {

  private final DSLContext context;
  private final ObjectMapper mapper;

  public List<DigitalMediaObjectRecord> getDigitalMediaObject(
      List<String> digitalSpecimenIds, List<String> mediaUrls) {
    return context.select(DIGITAL_MEDIA_OBJECT.asterisk())
        .from(DIGITAL_MEDIA_OBJECT)
        .where(DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID.in(digitalSpecimenIds))
        .and(DIGITAL_MEDIA_OBJECT.MEDIA_URL.in(mediaUrls))
        .fetch(this::mapDigitalMediaObject);
  }

  private DigitalMediaObjectRecord mapDigitalMediaObject(Record dbRecord) {
    DigitalMediaObject digitalMediaObject = null;
    try {
      digitalMediaObject = new DigitalMediaObject(
          dbRecord.get(DIGITAL_MEDIA_OBJECT.TYPE),
          dbRecord.get(DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID),
          mapToDigitalEntity(dbRecord.get(DIGITAL_MEDIA_OBJECT.DATA)),
          mapper.readTree(dbRecord.get(DIGITAL_MEDIA_OBJECT.ORIGINAL_DATA).data())
      );
    } catch (JsonProcessingException e) {
      log.error("Unable to map record data to json: {}", dbRecord);
    }
    return new DigitalMediaObjectRecord(
        dbRecord.get(DIGITAL_MEDIA_OBJECT.ID),
        dbRecord.get(DIGITAL_MEDIA_OBJECT.VERSION),
        dbRecord.get(DIGITAL_MEDIA_OBJECT.CREATED),
        digitalMediaObject);
  }

  private DigitalEntity mapToDigitalEntity(JSONB jsonb) throws JsonProcessingException {
    return mapper.readValue(jsonb.data(), DigitalEntity.class);
  }

  public int[] createDigitalMediaRecord(
      Collection<DigitalMediaObjectRecord> digitalSpecimenRecords) {
    var queries = digitalSpecimenRecords.stream().map(this::digitalMediaToQuery).toList();
    return context.batch(queries).execute();
  }


  public Query digitalMediaToQuery(DigitalMediaObjectRecord digitalMediaObjectRecord) {
    return context.insertInto(DIGITAL_MEDIA_OBJECT)
        .set(DIGITAL_MEDIA_OBJECT.ID, digitalMediaObjectRecord.id())
        .set(DIGITAL_MEDIA_OBJECT.TYPE, digitalMediaObjectRecord.digitalMediaObject().type())
        .set(DIGITAL_MEDIA_OBJECT.VERSION, digitalMediaObjectRecord.version())
        .set(DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID,
            digitalMediaObjectRecord.digitalMediaObject().digitalSpecimenId())
        .set(DIGITAL_MEDIA_OBJECT.MEDIA_URL,
            digitalMediaObjectRecord.digitalMediaObject().attributes().getAcAccessUri())
        .set(DIGITAL_MEDIA_OBJECT.CREATED, digitalMediaObjectRecord.created())
        .set(DIGITAL_MEDIA_OBJECT.LAST_CHECKED, Instant.now())
        .set(DIGITAL_MEDIA_OBJECT.DATA,
            JSONB.jsonb(
                mapper.valueToTree(digitalMediaObjectRecord.digitalMediaObject().attributes())
                    .toString()))
        .set(DIGITAL_MEDIA_OBJECT.ORIGINAL_DATA,
            JSONB.jsonb(
                digitalMediaObjectRecord.digitalMediaObject().originalAttributes().toString()))
        .onConflict(DIGITAL_MEDIA_OBJECT.ID).doUpdate()
        .set(DIGITAL_MEDIA_OBJECT.TYPE, digitalMediaObjectRecord.digitalMediaObject().type())
        .set(DIGITAL_MEDIA_OBJECT.VERSION, digitalMediaObjectRecord.version())
        .set(DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID,
            digitalMediaObjectRecord.digitalMediaObject().digitalSpecimenId())
        .set(DIGITAL_MEDIA_OBJECT.MEDIA_URL,
            digitalMediaObjectRecord.digitalMediaObject().attributes().getAcAccessUri())
        .set(DIGITAL_MEDIA_OBJECT.CREATED, digitalMediaObjectRecord.created())
        .set(DIGITAL_MEDIA_OBJECT.LAST_CHECKED, Instant.now())
        .set(DIGITAL_MEDIA_OBJECT.DATA,
            JSONB.jsonb(
                mapper.valueToTree(digitalMediaObjectRecord.digitalMediaObject().attributes())
                    .toString()))
        .set(DIGITAL_MEDIA_OBJECT.ORIGINAL_DATA,
            JSONB.jsonb(
                digitalMediaObjectRecord.digitalMediaObject().originalAttributes().toString()));
  }

  public void updateLastChecked(List<String> currentDigitalMediaObject) {
    context.update(DIGITAL_MEDIA_OBJECT)
        .set(DIGITAL_MEDIA_OBJECT.LAST_CHECKED, Instant.now())
        .where(DIGITAL_MEDIA_OBJECT.ID.in(currentDigitalMediaObject))
        .execute();
  }

  public void rollBackDigitalMedia(String id) {
    context.delete(DIGITAL_MEDIA_OBJECT).where(DIGITAL_MEDIA_OBJECT.ID.eq(id)).execute();
  }
}
