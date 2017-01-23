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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.plos.repo.models.RepoCollection;
import org.plos.repo.models.Status;
import org.plos.repo.util.TimestampFormatter;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * Collection to be return to the client
 */
@XmlRootElement
public class RepoCollectionOutput {

  private String key; // what the user specifies
  private String timestamp;   // created time
  private Integer versionNumber;
  private String tag;
  private String creationDate;
  private Status status;
  private List<RepoObjectOutput> objects;
  private String userMetadata;
  private String uuid;

  private RepoCollectionOutput() {
  }

  public RepoCollectionOutput(RepoCollection repoCollection) {
    this.key = repoCollection.getKey();
    this.timestamp = TimestampFormatter.getFormattedTimestamp(repoCollection.getTimestamp());
    this.versionNumber = repoCollection.getVersionNumber();
    this.tag = repoCollection.getTag();
    this.creationDate = TimestampFormatter.getFormattedTimestamp(repoCollection.getCreationDate());
    this.status = repoCollection.getStatus();
    this.userMetadata = repoCollection.getUserMetadata();

    if (repoCollection.getUuid() != null) {
      this.uuid = repoCollection.getUuid().toString();
    }

    if (repoCollection.getRepoObjects() != null && repoCollection.getRepoObjects().size() > 0) {
      this.objects = Lists.newArrayList(Iterables.transform(repoCollection.getRepoObjects(), RepoObjectOutput.typeFunction()));
    }
  }

  public void addObjects(List<RepoObjectOutput> repoObjectOutputs) {
    this.objects = repoObjectOutputs;
  }

  public String getKey() {
    return key;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public Integer getVersionNumber() {
    return versionNumber;
  }

  public List<RepoObjectOutput> getObjects() {
    return objects;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setVersionNumber(Integer versionNumber) {
    this.versionNumber = versionNumber;
  }

  public void setObjects(List<RepoObjectOutput> objects) {
    this.objects = objects;
  }

  public String getTag() {
    return tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public String getCreationDate() {
    return creationDate;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  public void setCreationDate(String creationDate) {
    this.creationDate = creationDate;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
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

  @Override
  public int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
  }

  @Override
  public boolean equals(java.lang.Object obj) {
    return EqualsBuilder.reflectionEquals(this, obj);
  }

  public static Function<RepoCollection, RepoCollectionOutput> typeFunction() {
    return new Function<RepoCollection, RepoCollectionOutput>() {
      @Override
      public RepoCollectionOutput apply(RepoCollection repoCollection) {
        return new RepoCollectionOutput(repoCollection);
      }
    };
  }

}
