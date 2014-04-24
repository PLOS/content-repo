package org.plos.repo.models;

import javax.xml.bind.annotation.XmlRootElement;
import java.lang.Integer;
import java.lang.String;
import java.lang.UnsupportedOperationException;

@XmlRootElement
public class Bucket {

  public Integer bucketId;
  public String bucketName;

  // empty constructor required for JAXB mapping
  public Bucket() {
    throw new UnsupportedOperationException("No-arg constructor should not be called");
  }

  public Bucket(Integer bucketId, String bucketName) {
    this.bucketName = bucketName;
    this.bucketId = bucketId;
  }

}
