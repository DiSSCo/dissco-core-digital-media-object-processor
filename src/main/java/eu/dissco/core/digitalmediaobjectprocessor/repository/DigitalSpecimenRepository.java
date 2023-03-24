package eu.dissco.core.digitalmediaobjectprocessor.repository;

import static eu.dissco.core.digitalmediaobjectprocessor.database.jooq.Tables.NEW_DIGITAL_SPECIMEN;

import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class DigitalSpecimenRepository {

  private final DSLContext context;

  public Map<String, String> getSpecimenId(List<String> physicalSpecimenIds) {
    return context.select(NEW_DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID, NEW_DIGITAL_SPECIMEN.ID)
        .from(NEW_DIGITAL_SPECIMEN)
        .where(NEW_DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID.in(physicalSpecimenIds))
        .fetchMap(Record2::value1, Record2::value2);
  }
}
