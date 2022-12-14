package eu.dissco.core.digitalmediaobjectprocessor.repository;

import static eu.dissco.core.digitalmediaobjectprocessor.database.jooq.Tables.NEW_DIGITAL_SPECIMEN;

import eu.dissco.core.digitalmediaobjectprocessor.domain.DigitalSpecimenInformation;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class DigitalSpecimenRepository {

  private final DSLContext context;


  public Optional<DigitalSpecimenInformation> getSpecimenIdBasedOnDWCAId(String dwcaId) {
    return context.select(NEW_DIGITAL_SPECIMEN.ID,
            NEW_DIGITAL_SPECIMEN.ORGANIZATION_ID)
        .distinctOn(NEW_DIGITAL_SPECIMEN.ID)
        .from(NEW_DIGITAL_SPECIMEN)
        .where(NEW_DIGITAL_SPECIMEN.DWCA_ID.eq(dwcaId))
        .orderBy(NEW_DIGITAL_SPECIMEN.ID, NEW_DIGITAL_SPECIMEN.VERSION.desc())
        .fetchOptional(this::mapToDigitalSpecimenInformation);
  }

  private DigitalSpecimenInformation mapToDigitalSpecimenInformation(Record dbRecord) {
    return new DigitalSpecimenInformation(
        dbRecord.get(NEW_DIGITAL_SPECIMEN.ID),
        dbRecord.get(NEW_DIGITAL_SPECIMEN.ORGANIZATION_ID)
    );
  }

  public Optional<DigitalSpecimenInformation> getSpecimenId(String physicalSpecimenId) {
    return context.select(NEW_DIGITAL_SPECIMEN.ID, NEW_DIGITAL_SPECIMEN.ORGANIZATION_ID)
        .distinctOn(NEW_DIGITAL_SPECIMEN.ID)
        .from(NEW_DIGITAL_SPECIMEN)
        .where(NEW_DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID.eq(physicalSpecimenId))
        .orderBy(NEW_DIGITAL_SPECIMEN.ID, NEW_DIGITAL_SPECIMEN.VERSION.desc())
        .fetchOptional(this::mapToDigitalSpecimenInformation);
  }
}
