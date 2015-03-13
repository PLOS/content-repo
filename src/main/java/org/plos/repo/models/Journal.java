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


public class Journal {
  
  private Integer id;
  private String bucket;
  private String key;
  private Operation operation;
  private UUID uuid;
  private Timestamp timestamp;


  public Journal(String bucket, String key, Operation operation, UUID uuid) {
    this.bucket = bucket;
    this.key = key;
    this.operation = operation;
    this.uuid = uuid;
  }
  
  public Journal(String bucket, Operation operation) {
    this.bucket = bucket;
    this.operation = operation;
  }

  public Integer getId() {
    return id;
  }

  public String getBucket() {
    return bucket;
  }

  public String getKey() {
    return key;
  }

  public Timestamp getTimestamp() {
    return timestamp;
  }

  public Operation getOperation() {
    return operation; 
  }

  public UUID getUuid() {
    return uuid;
  }

  public void setUuid(UUID uuid) {
    this.uuid = uuid;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public void setTimestamp(Timestamp timestamp) {
    this.timestamp = timestamp;
  }

  public void setOperation(Operation operation) { this.operation = operation; }
}
