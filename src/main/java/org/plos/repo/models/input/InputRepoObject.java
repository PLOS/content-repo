/*
 * Copyright (c) 2006-2015 by Public Library of Science
 *
 *    http://plos.org
 *    http://ambraproject.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.plos.repo.models.input;

import com.wordnik.swagger.annotations.ApiParam;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.InputStream;

/**
 * Input Repo Object
 */
@XmlRootElement
public class InputRepoObject {

  @ApiParam(required = true)
  @FormDataParam("key")
  private String key; // what the user specifies

  @ApiParam(required = true)
  @FormDataParam("bucketName")
  private String bucketName;

  @ApiParam(value = "MIME type")
  @FormDataParam("contentType")
  private String contentType;

  @ApiParam(value = "name of file when downloaded", required = false)
  @FormDataParam("downloadName")
  private String downloadName;

  @ApiParam(value = "creation method", allowableValues = "new,version,auto", defaultValue = "new",
      required = true)
  @FormDataParam("create")
  private String create;

  @ApiParam(value = "last modification time", required = false)
  @FormDataParam("timestamp")
  private String timestamp;   // created time

  @ApiParam(value = "creation time", required = false)
  @FormDataParam("creationDateTime")
  private String creationDateTime;   // created time

  @ApiParam(value = "creation time", required = false)
  @FormDataParam("tag")
  private String tag;

  @ApiParam(value = "user metadata", required = false)
  @FormDataParam("userMetadata")
  private String userMetadata;

  @ApiParam(required = false)
  @FormDataParam("file")
  private InputStream uploadedInputStream;

  public String getKey() {
    return key;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getContentType() {
    return contentType;
  }

  public String getDownloadName() {
    return downloadName;
  }

  public String getCreate() {
    return create;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public String getCreationDateTime() {
    return creationDateTime;
  }

  public String getTag() {
    return tag;
  }

  public String getUserMetadata() {
    return userMetadata;
  }

  public InputStream getUploadedInputStream() {
    return uploadedInputStream;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

  public void setDownloadName(String downloadName) {
    this.downloadName = downloadName;
  }

  public void setCreate(String create) {
    this.create = create;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  public void setCreationDateTime(String creationDateTime) {
    this.creationDateTime = creationDateTime;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public void setUserMetadata(String userMetadata) {
    this.userMetadata = userMetadata;
  }

  public void setUploadedInputStream(InputStream uploadedInputStream) {
    this.uploadedInputStream = uploadedInputStream;
  }
}
