package eu.dissco.core.digitalmediaobjectprocessor.exceptions;

import org.jooq.exception.DataAccessException;

public class DisscoJsonBMappingException extends DataAccessException {

  public DisscoJsonBMappingException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
