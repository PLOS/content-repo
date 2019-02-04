/*
 * Copyright (c) 2014-2019 Public Library of Science
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

  public boolean areSimilar(RepoObject repoObject) {
    return this.key.equals(repoObject.key) &&
        this.bucketName.equals(repoObject.bucketName) &&
        this.status.equals(repoObject.status) &&
        compareNullableElements(this.contentType, repoObject.contentType) &&
        compareNullableElements(this.downloadName, repoObject.downloadName) &&
        compareNullableElements(this.tag, repoObject.tag) &&
        compareNullableElements(this.checksum, repoObject.checksum) &&
        compareNullableElements(this.userMetadata, repoObject.getUserMetadata());
  }

  private boolean compareNullableElements(String string1, String string2) {
    if (string1 != null && string2 != null) {
      return string1.equals(string2);
    } else if (string1 == null && string2 == null) {
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

  public void setReproxyURL(URL[] reproxyURL) {
    this.reproxyURL = reproxyURL;
  }

  public String getUserMetadata() {
    return userMetadata;
  }

  public void setUserMetadata(String userMetadata) {
    if (userMetadata != null) {
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
