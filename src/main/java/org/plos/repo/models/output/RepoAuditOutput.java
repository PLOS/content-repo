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
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.hsqldb.lib.StringUtil;
import org.plos.repo.models.Audit;

import javax.xml.bind.annotation.XmlRootElement;
import java.sql.Timestamp;

/**
 * Audit record to be return to the client
 */
@XmlRootElement
public class RepoAuditOutput {

  private String bucket;
  private String key;
  private String operation;
  private String uuid;
  private Timestamp timestamp;

  private RepoAuditOutput(){}

  public RepoAuditOutput(Audit audit) {
    this.bucket = audit.getBucket();
    if (!StringUtil.isEmpty(audit.getKey())){
      this.key = audit.getKey();
    }
    this.operation = audit.getOperation().getValue();
    if (audit.getUuid() != null){
      this.uuid = audit.getUuid().toString();
    }
    this.timestamp = audit.getTimestamp();
  }

  public String getBucket() {
    return bucket;
  }

  public String getKey() {
    return key;
  }

  public String getOperation() {
    return operation;
  }

  public String getUuid() {
    return uuid;
  }

  public Timestamp getTimestamp() {
    return timestamp;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public void setTimestamp(Timestamp timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
  }

  @Override
  public boolean equals(java.lang.Object obj) {
    return EqualsBuilder.reflectionEquals(this, obj);
  }

  public static Function<Audit, RepoAuditOutput> typeFunction() {
    return new Function<Audit, RepoAuditOutput>() {
      @Override
      public RepoAuditOutput apply(Audit audit) {
        return new RepoAuditOutput(audit);
      }
    };
  }

}
