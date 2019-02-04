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
