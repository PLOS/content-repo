package org.plos.repo.models;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Bucket {

  public Integer bucketId;
  public String bucketName;

  public Integer activeObjects;   // used only by RepoInfoService
  public Integer totalObjects;    // used only by RepoInfoService

  // empty constructor required for JAXB mapping
  private Bucket() {
  }

  public Bucket(String bucketName) {
    this.bucketName = bucketName;
  }

  public Bucket(Integer bucketId, String bucketName) {
    this.bucketName = bucketName;
    this.bucketId = bucketId;
  }

}
