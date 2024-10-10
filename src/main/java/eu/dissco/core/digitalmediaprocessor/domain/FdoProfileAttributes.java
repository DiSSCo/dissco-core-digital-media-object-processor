package eu.dissco.core.digitalmediaprocessor.domain;

import lombok.Getter;

@Getter
public enum FdoProfileAttributes {

  MEDIA_HOST("mediaHost"),
  MEDIA_HOST_NAME("mediaHostName"),
  LINKED_DO_PID("linkedDigitalObjectPid"),
  LINKED_DO_TYPE("linkedDigitalObjectType"),
  MEDIA_ID("primaryMediaId"),
  MEDIA_ID_TYPE("primaryMediaIdType"),
  MEDIA_ID_NAME("primaryMediaIdName"),
  MEDIA_TYPE("mediaType"),
  MIME_TYPE("mimeType"),
  LICENSE_NAME("licenseName"),
  LICENSE_ID("licenseId"),
  RIGHTS_HOLDER_ID("rightsHolderId"),
  RIGHTS_HOLDER_NAME("rightsHolderName");

  private final String attribute;

  FdoProfileAttributes(String attribute) {
    this.attribute = attribute;
  }

}
