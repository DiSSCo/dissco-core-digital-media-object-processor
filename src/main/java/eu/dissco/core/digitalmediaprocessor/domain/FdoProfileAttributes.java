package eu.dissco.core.digitalmediaprocessor.domain;

import lombok.Getter;

@Getter
public enum FdoProfileAttributes {

  TYPE("type", null),
  ISSUED_FOR_AGENT("issuedForAgent", null),
  REFERENT_NAME("referentName", null),
  PRIMARY_MEDIA_ID("primaryMediaId", null),
  LINKED_DO_PID("linkedDigitalObjectPid", null),
  MEDIA_HOST("mediaHost", null),
  IS_DERIVED_FROM_SPECIMEN("isDerivedFromSpecimen", "true"),
  LINKED_DO_TYPE("linkedDigitalObjectType", "digital specimen"),
  PRIMARY_MO_ID_TYPE("primaryMediaObjectIdType", "Resolvable"),
  PRIMARY_MO_ID_NAME("primaryMediaObjectIdName", "ac:accessUri"),
  DCTERMS_TYPE("dcterms:type", "image"),
  LICENSE_NAME("licenseName", null),
  RIGHTSHOLDER_PID_TYPE("rightsholderPidType", "Resolvable"),
  MEDIA_FORMAT("dcterms:format", "image");

  private final String attribute;
  private final String defaultValue;

  FdoProfileAttributes(String attribute, String defaultValue) {
    this.attribute = attribute;
    this.defaultValue = defaultValue;
  }

}
