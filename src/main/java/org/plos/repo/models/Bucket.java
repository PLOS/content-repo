/*
 * Copyright (c) 2006-2014 by Public Library of Science
 * http://plos.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
