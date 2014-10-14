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

package org.plos.repo.models.output;

import com.google.common.base.Function;
import org.plos.repo.models.Status;
import org.plos.repo.util.TimestampFormatter;

import javax.xml.bind.annotation.XmlRootElement;
import java.sql.Timestamp;

/**
 * Collection to be return to the client
 */
@XmlRootElement
public class Object {

  private String key; // what the user specifies
  private String checksum;  // of the file contents

  private String timestamp;   // last modification time
  private String downloadName;
  private String contentType;
  private Long size;
  private String tag;
  private Integer versionNumber;
  private Status status;
  private String creationDate;
  private String versionChecksum;
  private String reproxyURL;

  private Object() {

  }

  public Object(org.plos.repo.models.Object object) {

    this.key = object.getKey();
    this.checksum = object.getChecksum();
    this.timestamp = TimestampFormatter.getFormattedTimestamp(object.getTimestamp());
    this.downloadName = object.getDownloadName();
    this.contentType = object.getContentType();
    this.size = object.getSize();
    this.tag = object.getTag();
    this.versionNumber = object.getVersionNumber();
    this.status = object.getStatus();
    this.creationDate = TimestampFormatter.getFormattedTimestamp(object.getCreationDate());
    this.versionChecksum = object.getVersionChecksum();

    if (object.getReproxyURL() != null ) {
      this.reproxyURL = object.getReproxyURL().toString();
    }

  }

  public String getKey() {
    return key;
  }

  public String getChecksum() {
    return checksum;
  }

  public String getTimestamp() {
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

  public Integer getVersionNumber() {
    return versionNumber;
  }

  public Status getStatus() {
    return status;
  }

  public String getCreationDate() {
    return creationDate;
  }

  public String getVersionChecksum() {
    return versionChecksum;
  }

  public String getReproxyURL() {
    return reproxyURL;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setChecksum(String checksum) {
    this.checksum = checksum;
  }

  public void setTimestamp(String timestamp) {
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

  public void setVersionNumber(Integer versionNumber) {
    this.versionNumber = versionNumber;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public void setCreationDate(String creationDate) {
    this.creationDate = creationDate;
  }

  public void setVersionChecksum(String versionChecksum) {
    this.versionChecksum = versionChecksum;
  }

  public void setReproxyURL(String reproxyURL) {
    this.reproxyURL = reproxyURL;
  }

  public static Function<org.plos.repo.models.Object, Object> typeFunction() {
    return new Function<org.plos.repo.models.Object, Object>() {

      @Override
      public Object apply(org.plos.repo.models.Object object) {
        return new Object(object);
      }

    };
  }

}
