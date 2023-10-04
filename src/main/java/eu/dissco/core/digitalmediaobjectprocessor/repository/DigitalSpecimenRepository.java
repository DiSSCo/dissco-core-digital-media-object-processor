package eu.dissco.core.digitalmediaobjectprocessor.repository;

import static eu.dissco.core.digitalmediaobjectprocessor.database.jooq.Tables.DIGITAL_SPECIMEN;

import java.util.HashSet;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class DigitalSpecimenRepository {

  private final DSLContext context;

  public Set<String> getExistingSpecimen(Set<String> digitalSpecimenIds) {
    return new HashSet<>(context.select(DIGITAL_SPECIMEN.ID)
        .from(DIGITAL_SPECIMEN)
        .where(DIGITAL_SPECIMEN.ID.in(digitalSpecimenIds))
        .fetch(DIGITAL_SPECIMEN.ID));
  }
}
