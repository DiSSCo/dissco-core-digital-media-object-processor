package eu.dissco.core.digitalmediaprocessor.repository;

import static eu.dissco.core.digitalmediaprocessor.database.jooq.Tables.DIGITAL_MEDIA_OBJECT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaWrapper;
import eu.dissco.core.digitalmediaprocessor.domain.DigitalMediaRecord;
import eu.dissco.core.digitalmediaprocessor.schema.DigitalMedia;
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
public class DigitalMediaRepository {

  private final DSLContext context;
  private final ObjectMapper mapper;

  public List<DigitalMediaRecord> getDigitalMedia(
      List<String> digitalSpecimenIds, List<String> mediaUrls) {
    return context.select(DIGITAL_MEDIA_OBJECT.asterisk())
        .from(DIGITAL_MEDIA_OBJECT)
        .where(DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID.in(digitalSpecimenIds))
        .and(DIGITAL_MEDIA_OBJECT.MEDIA_URL.in(mediaUrls))
        .fetch(this::mapDigitalMedia);
  }

  private DigitalMediaRecord mapDigitalMedia(Record dbRecord) {
    DigitalMediaWrapper digitalMediaWrapper = null;
    try {
      digitalMediaWrapper = new DigitalMediaWrapper(
          dbRecord.get(DIGITAL_MEDIA_OBJECT.TYPE),
          dbRecord.get(DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID),
          mapper.readValue(dbRecord.get(DIGITAL_MEDIA_OBJECT.DATA).data(), DigitalMedia.class),
          mapper.readTree(dbRecord.get(DIGITAL_MEDIA_OBJECT.ORIGINAL_DATA).data())
      );
    } catch (JsonProcessingException e) {
      log.error("Unable to map record data to json: {}", dbRecord);
    }
    return new DigitalMediaRecord(
        dbRecord.get(DIGITAL_MEDIA_OBJECT.ID),
        dbRecord.get(DIGITAL_MEDIA_OBJECT.VERSION),
        dbRecord.get(DIGITAL_MEDIA_OBJECT.CREATED),
        digitalMediaWrapper);
  }

  public int[] createDigitalMediaRecord(
      Collection<DigitalMediaRecord> digitalSpecimenRecords) {
    var queries = digitalSpecimenRecords.stream().map(this::digitalMediaToQuery).toList();
    return context.batch(queries).execute();
  }


  public Query digitalMediaToQuery(DigitalMediaRecord digitalMediaRecord) {
    return context.insertInto(DIGITAL_MEDIA_OBJECT)
        .set(DIGITAL_MEDIA_OBJECT.ID, digitalMediaRecord.id())
        .set(DIGITAL_MEDIA_OBJECT.TYPE, digitalMediaRecord.digitalMediaWrapper().type())
        .set(DIGITAL_MEDIA_OBJECT.VERSION, digitalMediaRecord.version())
        .set(DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID,
            digitalMediaRecord.digitalMediaWrapper().digitalSpecimenID())
        .set(DIGITAL_MEDIA_OBJECT.MEDIA_URL,
            digitalMediaRecord.digitalMediaWrapper().attributes().getAcAccessURI())
        .set(DIGITAL_MEDIA_OBJECT.CREATED, digitalMediaRecord.created())
        .set(DIGITAL_MEDIA_OBJECT.MODIFIED, Instant.now())
        .set(DIGITAL_MEDIA_OBJECT.LAST_CHECKED, Instant.now())
        .set(DIGITAL_MEDIA_OBJECT.DATA,
            JSONB.jsonb(
                mapper.valueToTree(digitalMediaRecord.digitalMediaWrapper().attributes())
                    .toString()))
        .set(DIGITAL_MEDIA_OBJECT.ORIGINAL_DATA,
            JSONB.jsonb(
                digitalMediaRecord.digitalMediaWrapper().originalAttributes().toString()))
        .onConflict(DIGITAL_MEDIA_OBJECT.ID).doUpdate()
        .set(DIGITAL_MEDIA_OBJECT.TYPE, digitalMediaRecord.digitalMediaWrapper().type())
        .set(DIGITAL_MEDIA_OBJECT.VERSION, digitalMediaRecord.version())
        .set(DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID,
            digitalMediaRecord.digitalMediaWrapper().digitalSpecimenID())
        .set(DIGITAL_MEDIA_OBJECT.MEDIA_URL,
            digitalMediaRecord.digitalMediaWrapper().attributes().getAcAccessURI())
        .set(DIGITAL_MEDIA_OBJECT.CREATED, digitalMediaRecord.created())
        .set(DIGITAL_MEDIA_OBJECT.MODIFIED, Instant.now())
        .set(DIGITAL_MEDIA_OBJECT.LAST_CHECKED, Instant.now())
        .set(DIGITAL_MEDIA_OBJECT.DATA,
            JSONB.jsonb(
                mapper.valueToTree(digitalMediaRecord.digitalMediaWrapper().attributes())
                    .toString()));
  }

  public void updateLastChecked(List<String> currentDigitalMedia) {
    context.update(DIGITAL_MEDIA_OBJECT)
        .set(DIGITAL_MEDIA_OBJECT.LAST_CHECKED, Instant.now())
        .where(DIGITAL_MEDIA_OBJECT.ID.in(currentDigitalMedia))
        .execute();
  }

  public void rollBackDigitalMedia(String id) {
    context.delete(DIGITAL_MEDIA_OBJECT).where(DIGITAL_MEDIA_OBJECT.ID.eq(id)).execute();
  }
}
