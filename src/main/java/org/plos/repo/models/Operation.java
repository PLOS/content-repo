package org.plos.repo.models;

/**
 * Represents an operation perform in the content repo, such as inserting or updating the DB
 */
public class Operation {

  private OperationType operationType;
  private String key;
  private String bucketName;
  private String version;

  public Operation(){

  }

  public Operation(OperationType operationType, String key, String bucketName, String version) {
    this.operationType = operationType;
    this.key = key;
    this.bucketName = bucketName;
    this.version = version;
  }

  public OperationType getOperationType() {
    return operationType;
  }

  public String getKey() {
    return key;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getVersion() {
    return version;
  }

  public void setOperationType(OperationType operationType) {
    this.operationType = operationType;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public void setVersion(String version) {
    this.version = version;
  }
}
