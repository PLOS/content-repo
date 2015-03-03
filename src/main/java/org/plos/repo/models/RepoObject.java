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

import org.plos.repo.models.input.InputRepoObject;

import java.net.URL;
import java.sql.Timestamp;
import java.util.UUID;


public class RepoObject {

  private Integer id; // assigned by the db
  private String key; // what the user specifies
  private String checksum;  // of the file contents
  private Timestamp timestamp;   // last modification time
  private String downloadName;
  private String contentType;
  private Long size;
  private String tag;
  private Integer bucketId;
  private String bucketName;
  private Integer versionNumber;
  private Status status;
  private Timestamp creationDate;
  private String versionChecksum;
  private URL[] reproxyURL;
  private String userMetadata;
  private UUID uuid;

  public RepoObject() {
  }

  public RepoObject(String key, Integer bucketId, String bucketName, Status status) {
    this.key = key;
    this.bucketId = bucketId;
    this.bucketName = bucketName;
    this.status = status;
  }

  public Boolean areSimilar(RepoObject repoObject){

      return this.key.equals(repoObject.key) &&
             this.bucketName.equals(repoObject.bucketName) &&
             this.status.equals(repoObject.status) &&
             compareNullableElements(this.contentType, repoObject.contentType) &&
             compareNullableElements(this.downloadName, repoObject.downloadName) &&
             compareNullableElements(this.tag, repoObject.tag) &&
             compareNullableElements(this.checksum, repoObject.checksum) &&
             compareNullableElements(this.userMetadata, repoObject.getUserMetadata())
          ;

  }

  private Boolean compareNullableElements(String string1, String string2){
    if (string1 != null && string2 != null) {
      return string1.equals(string2);
    } else if(string1 == null && string2 == null) {
      return true;
    } else {
      return false;
    }
  }

  public Integer getId() {
    return id;
  }

  public String getKey() {
    return key;
  }

  public String getChecksum() {
    return checksum;
  }

  public Timestamp getTimestamp() {
    return timestamp;
  }

  public String getDownloadName() {
    return downloadName;
  }

  public String getContentType() {
    return contentType;
  }

  public Long getSize() {
    return size;
  }

  public String getTag() {
    return tag;
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

  public Timestamp getCreationDate() {
    return creationDate;
  }

  public String getVersionChecksum() {
    return versionChecksum;
  }

  public URL[] getReproxyURL() {
    return reproxyURL;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setChecksum(String checksum) {
    this.checksum = checksum;
  }

  public void setTimestamp(Timestamp timestamp) {
    this.timestamp = timestamp;
  }

  public void setDownloadName(String downloadName) {
    this.downloadName = downloadName;
  }

  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

  public void setSize(Long size) {
    this.size = size;
  }

  public void setTag(String tag) {
    this.tag = tag;
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

  public void setCreationDate(Timestamp creationDate) {
    this.creationDate = creationDate;
  }

  public void setVersionChecksum(String versionChecksum) {
    this.versionChecksum = versionChecksum;
  }

  public void setReproxyURL(URL[] reproxyURL) {
    this.reproxyURL = reproxyURL;
  }

  public String getUserMetadata() {
    return userMetadata;
  }

  public void setUserMetadata(String userMetadata) {
    if (userMetadata != null){
      this.userMetadata = userMetadata.trim();
    }
  }

  public UUID getUuid() {
    return uuid;
  }

  public void setUuid(UUID uuid) {
    this.uuid = uuid;
  }
}
