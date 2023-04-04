package eu.dissco.core.digitalmediaobjectprocessor.repository;

import static eu.dissco.core.digitalmediaobjectprocessor.database.jooq.Tables.NEW_DIGITAL_MEDIA_OBJECT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObject;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectRecord;
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
    return context.select(NEW_DIGITAL_MEDIA_OBJECT.asterisk())
        .from(NEW_DIGITAL_MEDIA_OBJECT)
        .where(NEW_DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID.in(digitalSpecimenIds))
        .and(NEW_DIGITAL_MEDIA_OBJECT.MEDIA_URL.in(mediaUrls))
        .fetch(this::mapDigitalMediaObject);
  }

  private DigitalMediaObjectRecord mapDigitalMediaObject(Record dbRecord) {
    DigitalMediaObject digitalMediaObject = null;
    try {
      digitalMediaObject = new DigitalMediaObject(
          dbRecord.get(NEW_DIGITAL_MEDIA_OBJECT.TYPE),
          dbRecord.get(NEW_DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID),
          dbRecord.get(NEW_DIGITAL_MEDIA_OBJECT.PHYSICAL_SPECIMEN_ID),
          mapper.readTree(dbRecord.get(NEW_DIGITAL_MEDIA_OBJECT.DATA).data()),
          mapper.readTree(dbRecord.get(NEW_DIGITAL_MEDIA_OBJECT.ORIGINAL_DATA).data())
      );
    } catch (JsonProcessingException e) {
      log.error("Unable to map record data to json: {}", dbRecord);
    }
    return new DigitalMediaObjectRecord(
        dbRecord.get(NEW_DIGITAL_MEDIA_OBJECT.ID),
        dbRecord.get(NEW_DIGITAL_MEDIA_OBJECT.VERSION),
        dbRecord.get(NEW_DIGITAL_MEDIA_OBJECT.CREATED),
        digitalMediaObject);
  }

  public int[] createDigitalMediaRecord(
      Collection<DigitalMediaObjectRecord> digitalSpecimenRecords) {
    var queries = digitalSpecimenRecords.stream().map(this::digitalMediaToQuery).toList();
    return context.batch(queries).execute();
  }


  public Query digitalMediaToQuery(DigitalMediaObjectRecord digitalMediaObjectRecord) {
    return context.insertInto(NEW_DIGITAL_MEDIA_OBJECT)
        .set(NEW_DIGITAL_MEDIA_OBJECT.ID, digitalMediaObjectRecord.id())
        .set(NEW_DIGITAL_MEDIA_OBJECT.TYPE, digitalMediaObjectRecord.digitalMediaObject().type())
        .set(NEW_DIGITAL_MEDIA_OBJECT.VERSION, digitalMediaObjectRecord.version())
        .set(NEW_DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID,
            digitalMediaObjectRecord.digitalMediaObject().digitalSpecimenId())
        .set(NEW_DIGITAL_MEDIA_OBJECT.PHYSICAL_SPECIMEN_ID,
            digitalMediaObjectRecord.digitalMediaObject().physicalSpecimenId())
        .set(NEW_DIGITAL_MEDIA_OBJECT.MEDIA_URL,
            digitalMediaObjectRecord.digitalMediaObject().attributes().get("ac:accessURI").asText())
        .set(NEW_DIGITAL_MEDIA_OBJECT.FORMAT,
            digitalMediaObjectRecord.digitalMediaObject().attributes().get("dcterms:format")
                .asText())
        .set(NEW_DIGITAL_MEDIA_OBJECT.SOURCE_SYSTEM_ID,
            digitalMediaObjectRecord.digitalMediaObject().attributes().get("ods:sourceSystemId")
                .asText())
        .set(NEW_DIGITAL_MEDIA_OBJECT.CREATED, digitalMediaObjectRecord.created())
        .set(NEW_DIGITAL_MEDIA_OBJECT.LAST_CHECKED, Instant.now())
        .set(NEW_DIGITAL_MEDIA_OBJECT.DATA,
            JSONB.jsonb(digitalMediaObjectRecord.digitalMediaObject().attributes().toString()))
        .set(NEW_DIGITAL_MEDIA_OBJECT.ORIGINAL_DATA,
            JSONB.jsonb(
                digitalMediaObjectRecord.digitalMediaObject().originalAttributes().toString()))
        .onConflict(NEW_DIGITAL_MEDIA_OBJECT.ID).doUpdate()
        .set(NEW_DIGITAL_MEDIA_OBJECT.TYPE, digitalMediaObjectRecord.digitalMediaObject().type())
        .set(NEW_DIGITAL_MEDIA_OBJECT.VERSION, digitalMediaObjectRecord.version())
        .set(NEW_DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID,
            digitalMediaObjectRecord.digitalMediaObject().digitalSpecimenId())
        .set(NEW_DIGITAL_MEDIA_OBJECT.PHYSICAL_SPECIMEN_ID,
            digitalMediaObjectRecord.digitalMediaObject().physicalSpecimenId())
        .set(NEW_DIGITAL_MEDIA_OBJECT.MEDIA_URL,
            digitalMediaObjectRecord.digitalMediaObject().attributes().get("ac:accessURI").asText())
        .set(NEW_DIGITAL_MEDIA_OBJECT.FORMAT,
            digitalMediaObjectRecord.digitalMediaObject().attributes().get("dcterms:format")
                .asText())
        .set(NEW_DIGITAL_MEDIA_OBJECT.SOURCE_SYSTEM_ID,
            digitalMediaObjectRecord.digitalMediaObject().attributes().get("ods:sourceSystemId")
                .asText())
        .set(NEW_DIGITAL_MEDIA_OBJECT.CREATED, digitalMediaObjectRecord.created())
        .set(NEW_DIGITAL_MEDIA_OBJECT.LAST_CHECKED, Instant.now())
        .set(NEW_DIGITAL_MEDIA_OBJECT.DATA,
            JSONB.jsonb(digitalMediaObjectRecord.digitalMediaObject().attributes().toString()))
        .set(NEW_DIGITAL_MEDIA_OBJECT.ORIGINAL_DATA,
            JSONB.jsonb(
                digitalMediaObjectRecord.digitalMediaObject().originalAttributes().toString()));
  }

  public void updateLastChecked(List<String> currentDigitalMediaObject) {
    context.update(NEW_DIGITAL_MEDIA_OBJECT)
        .set(NEW_DIGITAL_MEDIA_OBJECT.LAST_CHECKED, Instant.now())
        .where(NEW_DIGITAL_MEDIA_OBJECT.ID.in(currentDigitalMediaObject))
        .execute();
  }

  public void rollBackDigitalMedia(String id) {
    context.delete(NEW_DIGITAL_MEDIA_OBJECT).where(NEW_DIGITAL_MEDIA_OBJECT.ID.eq(id)).execute();
  }
}
