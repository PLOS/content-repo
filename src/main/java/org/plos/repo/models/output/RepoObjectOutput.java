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

package org.plos.repo.models.output;

import com.google.common.base.Function;
import org.plos.repo.models.RepoObject;
import org.plos.repo.models.Status;
import org.plos.repo.util.TimestampFormatter;

import javax.xml.bind.annotation.XmlRootElement;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

/**
 * Collection to be return to the client
 */
@XmlRootElement
public class RepoObjectOutput {

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
  private String userMetadata;
  private List<URL> reproxyURL;
  private String uuid;


  private RepoObjectOutput() {
  }

  public RepoObjectOutput(RepoObject repoObject) {
    this.key = repoObject.getKey();
    this.checksum = repoObject.getChecksum();
    this.timestamp = TimestampFormatter.getFormattedTimestamp(repoObject.getTimestamp());
    this.downloadName = repoObject.getDownloadName();
    this.contentType = repoObject.getContentType();
    this.size = repoObject.getSize();
    this.tag = repoObject.getTag();
    this.versionNumber = repoObject.getVersionNumber();
    this.status = repoObject.getStatus();
    this.creationDate = TimestampFormatter.getFormattedTimestamp(repoObject.getCreationDate());
    this.userMetadata = repoObject.getUserMetadata();

    if (repoObject.getUuid() != null) {
      this.uuid = repoObject.getUuid().toString();
    }

    URL[] urls = repoObject.getReproxyURL();

    if (urls != null) {
      reproxyURL = Arrays.asList(urls);
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

  public List<URL> getReproxyURL() {
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

  public void setReproxyURL(List<URL> reproxyURL) {
    this.reproxyURL = reproxyURL;
  }

  public String getUserMetadata() {
    return userMetadata;
  }

  public void setUserMetadata(String userMetadata) {
    this.userMetadata = userMetadata;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public static Function<RepoObject, RepoObjectOutput> typeFunction() {
    return new Function<RepoObject, RepoObjectOutput>() {
      @Override
      public RepoObjectOutput apply(RepoObject repoObject) {
        return new RepoObjectOutput(repoObject);
      }
    };
  }

}
