package org.plos.repo.models;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Bucket {
  public Integer bucketId;
  public String bucketName;

  // empty constructor required for JAXB mapping
  public Bucket() {
    throw new UnsupportedOperationException("No-arg constructor should not be called");
  }

  public Bucket(String bucketName, Integer bucketId) {
    this.bucketName = bucketName;
    this.bucketId = bucketId;
  }

  public Bucket(String bucketName) {
    this.bucketName = bucketName;
  }
}
