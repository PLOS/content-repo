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
