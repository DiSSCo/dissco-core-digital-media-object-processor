package eu.dissco.core.digitalmediaobjectprocessor.domain;

public enum FdoProfileAttributes {

  TYPE("type", "mediaObject"),
  FDO_PROFILE("fdoProfile", "https://hdl.handle.net/21.T11148/64396cf36b976ad08267"),
  DIGITAL_OBJECT_TYPE("digitalObjectType", "https://hdl.handle.net/21.T11148/bbad8c4e101e8af01115"),
  // Issued for agent should be DiSSCo PID; currently it's set as Naturalis's ROR
  ISSUED_FOR_AGENT("issuedForAgent", "https://ror.org/0566bfb96"),
  REFERENT_NAME("referentName", null),
  PRIMARY_MEDIA_ID("primaryMediaId", null),
  LINKED_DO_PID("linkedDigitalObjectPid", null),
  MEDIA_HOST("mediaHost", null),
  IS_DERIVED_FROM_SPECIMEN("isDerivedFromSpecimen", "true"),
  LINKED_DO_TYPE("linkedDigitalObjectType", "digital specimen"),
  PRIMARY_MO_ID_TYPE("primaryMediaObjectIdType", "Resolvable"),
  PRIMARY_MO_ID_NAME("primaryMediaObjectIdName", "ac:accessUri"),
  PRIMARY_MO_TYPE("primaryMediaObjectType", null),
  LICENSE("license", null),
  RIGHTSHOLDER_PID_TYPE("rightsholderPidType", "Resolvable"),
  MEDIA_FORMAT("mediaFormat", "image");


  private final String attribute;
  private final String defaultValue;

  FdoProfileAttributes(String attribute, String defaultValue) {
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
