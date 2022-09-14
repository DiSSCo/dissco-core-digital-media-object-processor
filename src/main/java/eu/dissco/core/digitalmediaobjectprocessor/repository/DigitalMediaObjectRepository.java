package eu.dissco.core.digitalmediaobjectprocessor.repository;

import static eu.dissco.core.digitalmediaobjectprocessor.database.jooq.Tables.NEW_DIGITAL_MEDIA_OBJECT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObject;
import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalMediaObjectRecord;
import java.time.Instant;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Record;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class DigitalMediaObjectRepository {

  private final DSLContext context;
  private final ObjectMapper mapper;

  public Optional<DigitalMediaObjectRecord> getDigitalMediaObject(String digitalSpecimenId,
      String mediaUrl) {
    return context.select(NEW_DIGITAL_MEDIA_OBJECT.asterisk())
        .distinctOn(NEW_DIGITAL_MEDIA_OBJECT.ID)
        .from(NEW_DIGITAL_MEDIA_OBJECT)
        .where(NEW_DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID.eq(digitalSpecimenId))
        .and(NEW_DIGITAL_MEDIA_OBJECT.MEDIA_URL.eq(mediaUrl))
        .orderBy(NEW_DIGITAL_MEDIA_OBJECT.ID, NEW_DIGITAL_MEDIA_OBJECT.VERSION.desc())
        .fetchOptional(this::mapDigitalMediaObject);
  }

  private DigitalMediaObjectRecord mapDigitalMediaObject(Record dbRecord) {
    DigitalMediaObject digitalMediaObject = null;
    try {
      digitalMediaObject = new DigitalMediaObject(
          dbRecord.get(NEW_DIGITAL_MEDIA_OBJECT.TYPE),
          dbRecord.get(NEW_DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID),
          dbRecord.get(NEW_DIGITAL_MEDIA_OBJECT.MEDIA_URL),
          dbRecord.get(NEW_DIGITAL_MEDIA_OBJECT.FORMAT),
          dbRecord.get(NEW_DIGITAL_MEDIA_OBJECT.SOURCE_SYSTEM_ID),
          mapper.readTree(dbRecord.get(NEW_DIGITAL_MEDIA_OBJECT.DATA).data()),
          mapper.readTree(dbRecord.get(NEW_DIGITAL_MEDIA_OBJECT.ORIGINAL_DATA).data())
      );
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    return new DigitalMediaObjectRecord(dbRecord.get(NEW_DIGITAL_MEDIA_OBJECT.ID),
        dbRecord.get(NEW_DIGITAL_MEDIA_OBJECT.VERSION),
        digitalMediaObject);
  }

  public int createDigitalMediaObjectRecord(DigitalMediaObjectRecord digitalMediaObjectRecord) {
    return context.insertInto(NEW_DIGITAL_MEDIA_OBJECT)
        .set(NEW_DIGITAL_MEDIA_OBJECT.ID, digitalMediaObjectRecord.id())
        .set(NEW_DIGITAL_MEDIA_OBJECT.TYPE, digitalMediaObjectRecord.digitalMediaObject().type())
        .set(NEW_DIGITAL_MEDIA_OBJECT.VERSION, digitalMediaObjectRecord.version())
        .set(NEW_DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID,
            digitalMediaObjectRecord.digitalMediaObject().digitalSpecimenId())
        .set(NEW_DIGITAL_MEDIA_OBJECT.MEDIA_URL,
            digitalMediaObjectRecord.digitalMediaObject().mediaUrl())
        .set(NEW_DIGITAL_MEDIA_OBJECT.FORMAT,
            digitalMediaObjectRecord.digitalMediaObject().format())
        .set(NEW_DIGITAL_MEDIA_OBJECT.SOURCE_SYSTEM_ID,
            digitalMediaObjectRecord.digitalMediaObject().sourceSystemId())
        .set(NEW_DIGITAL_MEDIA_OBJECT.CREATED, Instant.now())
        .set(NEW_DIGITAL_MEDIA_OBJECT.LAST_CHECKED, Instant.now())
        .set(NEW_DIGITAL_MEDIA_OBJECT.DATA,
            JSONB.jsonb(digitalMediaObjectRecord.digitalMediaObject().data().toString()))
        .set(NEW_DIGITAL_MEDIA_OBJECT.ORIGINAL_DATA,
            JSONB.jsonb(digitalMediaObjectRecord.digitalMediaObject().originalData().toString()))
        .execute();
  }

  public int updateLastChecked(DigitalMediaObjectRecord currentDigitalMediaObject) {
    return context.update(NEW_DIGITAL_MEDIA_OBJECT)
        .set(NEW_DIGITAL_MEDIA_OBJECT.LAST_CHECKED, Instant.now())
        .where(NEW_DIGITAL_MEDIA_OBJECT.ID.eq(currentDigitalMediaObject.id()))
        .execute();
  }

}
