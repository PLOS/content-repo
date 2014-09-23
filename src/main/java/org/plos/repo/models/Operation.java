package org.plos.repo.models;

import java.sql.Timestamp;

/**
 * Represents an operation perform in the content repo, such as inserting or updating the DB
 */
public class Operation {

  private ElementType elementType;
  private Status status;
  private String key;
  private String bucketName;
  private Integer version;
  private Timestamp lastModification;
  private Timestamp creationDateTime;

  public Operation(){

  }

  public Operation(Bucket bucket){
    this.elementType = ElementType.BUCKET;
    this.bucketName = bucket.bucketName;
    this.lastModification = bucket.timestamp;
  }

  public Operation(Object object, Status status){
    this.elementType = ElementType.OBJECT;
    this.key = object.key;
    this.bucketName = object.bucketName;
    this.version = object.versionNumber;
    if (Status.DELETED.equals(status)){
      this.lastModification = object.timestamp;
    } else {
      this.lastModification = object.creationDate;
    }
    this.creationDateTime = object.creationDate;
  }

  public Operation(Collection collection, Status status){
    this.elementType = ElementType.COLLECTION;
    this.key = collection.getKey();
    this.bucketName = collection.getBucketName();
    this.version = collection.getVersionNumber();
    this.lastModification = collection.getTimestamp();
    if (Status.DELETED.equals(status)){
      this.lastModification = collection.getTimestamp();
    } else {
      this.lastModification = collection.getCreationDate();
    }
    this.creationDateTime = collection.getCreationDate();
  }

  public ElementType getElementType() {
    return elementType;
  }

  public String getKey() {
    return key;
  }

  public String getBucketName() {
    return bucketName;
  }

  public Integer getVersion() {
    return version;
  }

  public void setElementType(ElementType elementType) {
    this.elementType = elementType;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

  public Timestamp getLastModification() {
    return lastModification;
  }

  public void setLastModification(Timestamp lastModification) {
    this.lastModification = lastModification;
  }

  public Status getStatus() {
    return status;
  }

  public Timestamp getCreationDateTime() {
    return creationDateTime;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public void setCreationDateTime(Timestamp creationDateTime) {
    this.creationDateTime = creationDateTime;
  }
}
