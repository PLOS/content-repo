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

package org.plos.repo.models.input;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * Collection of objects.
 */
@XmlRootElement
public class InputCollection {

  private String key; // what the user specifies
  private String timestamp;   // created time
  private String bucketName;
  private String create;
  private String tag;
  private List<InputObject> objects;
  private String creationDateTime;   // created time
  private String userMetadata;

  // empty constructor required for JAXB mapping
  public InputCollection() {
  }

  public String getKey() {
    return key;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getCreate() {
    return create;
  }

  public List<InputObject> getObjects() {
    return objects;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public void setCreate(String create) {
    this.create = create;
  }

  public void setObjects(List<InputObject> objects) {
    this.objects = objects;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  public String getTag() {
    return tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public String getCreationDateTime() {
    return creationDateTime;
  }

  public void setCreationDateTime(String creationDateTime) {
    this.creationDateTime = creationDateTime;
  }

  public String getUserMetadata() {
    return userMetadata;
  }

  public void setUserMetadata(String userMetadata) {
    this.userMetadata = userMetadata;
  }

}
