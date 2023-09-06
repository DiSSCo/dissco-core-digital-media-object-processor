package eu.dissco.core.digitalmediaobjectprocessor.domain;

public enum FdoProfileAttributes {

  TYPE("type", "mediaObject"),
  FDO_PROFILE("fdoProfile", "https://hdl.handle.net/21.T11148/64396cf36b976ad08267"),
  DIGITAL_OBJECT_TYPE("digitalObjectType", "https://hdl.handle.net/21.T11148/64396cf36b976ad08267"),
  // Issued for agent should be DiSSCo PID; currently it's set as Naturalis's ROR
  ISSUED_FOR_AGENT("issuedForAgent", "https://ror.org/0566bfb96"),
  REFERENT_NAME("referentName", null),
  PRIMARY_MEDIA_ID("primaryMediaId", null),
  LINKED_DO_PID("linkedDigitalObjectPid", null),
  MEDIA_HOST("mediaHost", null),
  MEDIA_HOST_NAME("mediaHostName", null),
  IS_DERIVED_FROM_SPECIMEN("isDerivedFromSpecimen", null),
  LINKED_DO_TYPE("linkedDigitalObjectType", null),
  LINKED_ATTRIBUTE("linkedAttribute", null),
  PRIMARY_MO_ID_TYPE("primaryMediaObjectIdType", null),
  PRIMARY_MO_ID_NAME("primaryMediaObjectIdName", null),
  PRIMARY_MO_TYPE("primaryMediaObjectType", null),
  MEDIA_MIME_TYPE("mediaMimeType", null),
  DERIVED_FROM_ENTITY("derivedFromEntity", null),
  LICENSE_NAME("licenseName", null),
  LICENSE_URL("licenseUrl", null),
  RIGHTSHOLDER_NAME("rightsholderName", null),
  RIGHTSHOLDER_PID("rigthsholderPid", null),
  RIGHTSHOLDER_PID_TYPE("rightsholderPidType", null),
  DC_TERMS_CONFORMS("dcterms:conforms", null),
  MEDIA_FORMAT("mediaFormat", null),;



  private final String attribute;
  private final String defaultValue;

  private FdoProfileAttributes(String attribute, String defaultValue) {
    this.attribute = attribute;
    this.defaultValue = defaultValue;
  }

  public String getAttribute() {
    return attribute;
  }

  public String getDefaultValue() {
    return defaultValue;
  }
}
