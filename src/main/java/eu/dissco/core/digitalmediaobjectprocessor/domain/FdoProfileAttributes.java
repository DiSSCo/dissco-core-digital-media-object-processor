package eu.dissco.core.digitalmediaobjectprocessor.domain;

public enum FdoProfileAttributes {

  TYPE("type", "mediaObject"),
  FDO_PROFILE("fdoProfile", "https://hdl.handle.net/21.T11148/64396cf36b976ad08267"),
  DIGITAL_OBJECT_TYPE("digitalObjectType", "https://hdl.handle.net/21.T11148/64396cf36b976ad08267"),
  // Issued for agent should be DiSSCo PID; currently it's set as Naturalis's ROR
  ISSUED_FOR_AGENT("issuedForAgent", "https://ror.org/0566bfb96"),
  REFERENT_NAME("referentName", null),
  MEDIA_URL("mediaUrl", null),

  SUBJECT_ID("subjectId", null);

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
