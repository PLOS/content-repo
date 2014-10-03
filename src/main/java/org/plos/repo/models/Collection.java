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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.sql.Timestamp;
import java.util.List;

/**
 * Collection of objects.
 */
public class Collection {

  private Integer id; // assigned by the db
  private String key; // what the user specifies
  private Timestamp timestamp;   // created time
  private Integer bucketId;
  private String bucketName;
  private Integer versionNumber;
  private Status status;
  private String tag;
  private Timestamp creationDate;
  private String versionChecksum;

  private List<Object> objects;

  private List<Collection> versions;

  // empty constructor required for JAXB mapping
  public Collection() {
  }

  public Collection(Integer id, String key, Timestamp timestamp, Integer bucketId, String bucketName,
                    Integer versionNumber, Status status, String tag, Timestamp creationDate, String versionChecksum) {
    this.id = id;
    this.key = key;
    this.timestamp = timestamp;
    this.bucketId = bucketId;
    this.bucketName = bucketName;
    this.versionNumber = versionNumber;
    this.status = status;
    this.tag = tag;
    this.creationDate = creationDate;
    this.versionChecksum = versionChecksum;
  }

  public Integer getId(){
    return id;
  }

  public void addObjects(List<Object> objects){

    this.objects = objects;
  }

  public String getKey() {
    return key;
  }

  public Timestamp getTimestamp() {
    return timestamp;
  }

  public Integer getBucketId() {
    return bucketId;
  }

  public String getBucketName() {
    return bucketName;
  }

  public Integer getVersionNumber() {
    return versionNumber;
  }

  public Status getStatus() {
    return status;
  }

  public List<Object> getObjects() {
    return objects;
  }

  public List<Collection> getVersions() {
    return versions;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setTimestamp(Timestamp timestamp) {
    this.timestamp = timestamp;
  }

  public void setBucketId(Integer bucketId) {
    this.bucketId = bucketId;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public void setVersionNumber(Integer versionNumber) {
    this.versionNumber = versionNumber;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public void setObjects(List<Object> objects) {
    this.objects = objects;
  }

  public void setVersions(List<Collection> versions) {
    this.versions = versions;
  }

  public String getTag() {
    return tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public Timestamp getCreationDate() {
    return creationDate;
  }

  public void setCreationDate(Timestamp creationDate) {
    this.creationDate = creationDate;
  }

  public String getVersionChecksum() {
    return versionChecksum;
  }

  public void setVersionChecksum(String versionChecksum) {
    this.versionChecksum = versionChecksum;
  }

  public boolean equals(Object o) {
    return EqualsBuilder.reflectionEquals(this, o);
  }

  @Override
  public int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
  }
}
