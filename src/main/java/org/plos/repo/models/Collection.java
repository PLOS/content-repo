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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * Collection of objects.
 */
@XmlRootElement
public class Collection {

  public enum Status {
    USED(0), DELETED(1);

    private final int value;

    private Status(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  public static final ImmutableMap<Integer, Status> STATUS_VALUES = Maps.uniqueIndex(EnumSet.allOf(Status.class),
      new Function<Status, Integer>() {
        @Override
        public Integer apply(Status status) {
          return status.getValue();
        }
      });

  private Integer id; // assigned by the db
  private String key; // what the user specifies
  @XmlJavaTypeAdapter(TimestampAdapter.class)
  private Timestamp timestamp;   // created time
  private Integer bucketId;
  private String bucketName;
  private Integer versionNumber;
  private Status status;

  private List<Object> objects;

  private List<Collection> versions;

  // empty constructor required for JAXB mapping
  public Collection() {
  }

  public Collection(Integer id, String key, Timestamp timestamp, Integer bucketId, String bucketName, Integer versionNumber, Status status) {
    this.id = id;
    this.key = key;
    this.timestamp = timestamp;
    this.bucketId = bucketId;
    this.bucketName = bucketName;
    this.versionNumber = versionNumber;
    this.status = status;
    this.versions = new ArrayList<Collection>();
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
}
