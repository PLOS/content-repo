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
