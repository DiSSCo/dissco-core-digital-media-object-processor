package eu.dissco.core.digitalmediaobjectprocessor.domain;

import lombok.Getter;

@Getter
public enum FdoProfileAttributes {

  TYPE("type", "https://hdl.handle.net/21.T11148/bbad8c4e101e8af01115"),
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
  PRIMARY_MO_TYPE("dcterms:type", null),
  LICENSE("license", null),
  RIGHTSHOLDER_PID_TYPE("rightsholderPidType", "Resolvable"),
  MEDIA_FORMAT("dcterms:format", "image");

  private final String attribute;
  private final String defaultValue;

  FdoProfileAttributes(String attribute, String defaultValue) {
    this.attribute = attribute;
    this.defaultValue = defaultValue;
  }

}
