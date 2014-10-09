package org.plos.repo.models.output;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Bucket {

  private String bucketName;
  private Long bytes;

  // empty constructor required for JAXB mapping
  public Bucket() {
  }

  public Bucket(String bucketName, Long size){
    this.bucketName = bucketName;
    this.bytes = size;
  }

  public String getBucketName() {
    return bucketName;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public Long getBytes() {
    return bytes;
  }

  public void setBytes(Long bytes) {
    this.bytes = bytes;
  }
}
