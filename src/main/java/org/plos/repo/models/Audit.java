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

import java.sql.Timestamp;
import java.util.UUID;

/**
 * Represents the operation's values to audit
 */
public class Audit {

  // All final attributes
  private final Integer id;
  private final String bucket;
  private final String key;
  private final Operation operation;
  private final UUID uuid;
  private final Timestamp timestamp;

  private Audit(AuditBuilder auditBuilder) {
    this.id = auditBuilder.id;
    this.bucket = auditBuilder.bucket;
    this.key = auditBuilder.key;
    this.operation = auditBuilder.operation;
    this.uuid = auditBuilder.uuid;
    this.timestamp = auditBuilder.timestamp;
  }

  // All getter, and NO setter to provide immutability
  public Integer getId() {
    return id;
  }

  public String getBucket() {
    return bucket;
  }

  public String getKey() {
    return key;
  }

  public Operation getOperation() {
    return operation;
  }

  public UUID getUuid() {
    return uuid;
  }

  public Timestamp getTimestamp() {
    return timestamp;
  }

  /**
   * Builder pattern implementation for Audit objects
   */
  public static class AuditBuilder {
    private final String bucket; // required
    private final Operation operation; // required
    private Integer id;
    private String key;
    private UUID uuid;
    private Timestamp timestamp;

    public AuditBuilder(String bucket, Operation operation) {
      this.bucket = bucket;
      this.operation = operation;
    }

    public AuditBuilder setId(Integer id) {
      this.id = id;
      return this;
    }

    public AuditBuilder setKey(String key) {
      this.key = key;
      return this;
    }

    public AuditBuilder setUuid(UUID uuid) {
      this.uuid = uuid;
      return this;
    }

    public AuditBuilder setTimestamp(Timestamp timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    /**
     * Return the finally constructed Audit object
     *
     * @return Audit object
     */
    public Audit build() {
      return new Audit(this);
    }
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder(" Audit { ");
    result.append(" BucketName: ").append(this.bucket).append(",");
    result.append(" Key: ").append(this.key).append(",");
    result.append(" Operation: ").append(this.operation.getValue()).append(",");
    result.append(" Uuid: ").append(this.uuid).append(" } ");
    return result.toString();
  }

}
