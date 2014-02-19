package org.plos.repo.models;

public class Bucket {
  public Integer bucketId;
  public String bucketName;

  public Bucket(String bucketName, Integer bucketId) {
    this.bucketName = bucketName;
    this.bucketId = bucketId;
  }

  public Bucket(String bucketName) {
    this.bucketName = bucketName;
  }
}
