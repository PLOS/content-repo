/*
 * Copyright (c) 2017 Public Library of Science
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

package org.plos.repo.models;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.sql.Timestamp;

@XmlRootElement
public class Bucket {

  private Integer bucketId;
  private String bucketName;
  @XmlJavaTypeAdapter(TimestampAdapter.class)
  private Timestamp timestamp;
  @XmlJavaTypeAdapter(TimestampAdapter.class)
  private Timestamp creationDate;

  private Integer activeObjects;   // used only by RepoInfoService
  private Integer totalObjects;    // used only by RepoInfoService

  // empty constructor required for JAXB mapping
  private Bucket() {
  }

  public Bucket(String bucketName) {
    this.bucketName = bucketName;
  }

  public Bucket(Integer bucketId, String bucketName, Timestamp timestamp, Timestamp creationDate) {
    this.bucketName = bucketName;
    this.bucketId = bucketId;
    this.timestamp = timestamp;
    this.creationDate = creationDate;
  }

  public Integer getBucketId() {
    return bucketId;
  }

  public String getBucketName() {
    return bucketName;
  }

  public Timestamp getTimestamp() {
    return timestamp;
  }

  public Timestamp getCreationDate() {
    return creationDate;
  }

  public Integer getActiveObjects() {
    return activeObjects;
  }

  public Integer getTotalObjects() {
    return totalObjects;
  }

  public void setBucketId(Integer bucketId) {
    this.bucketId = bucketId;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public void setTimestamp(Timestamp timestamp) {
    this.timestamp = timestamp;
  }

  public void setCreationDate(Timestamp creationDate) {
    this.creationDate = creationDate;
  }

  public void setActiveObjects(Integer activeObjects) {
    this.activeObjects = activeObjects;
  }

  public void setTotalObjects(Integer totalObjects) {
    this.totalObjects = totalObjects;
  }

}
