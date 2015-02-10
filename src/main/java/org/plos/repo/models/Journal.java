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


public class Journal {
  
  private Integer id;
  private String bucketName;
  private String objKey;
  private String collKey;
  private Operation operation;
  private String versionChecksum;
  private Timestamp timestamp;
  
  public Journal(String bucketName, String objKey, String collKey, Operation operation, String versionChecksum) {
    this.bucketName = bucketName;
    this.objKey = objKey;
    this.collKey = collKey;
    this.operation = operation;
    this.versionChecksum = versionChecksum;
  }
  
  public Journal(String bucketName, Operation operation) {
    this.bucketName = bucketName;
    this.operation = operation;
  }

  public Integer getId() {
    return id;
  }

  public String getBucketName() {
    return bucketName;
  }

  public Timestamp getTimestamp() {
    return timestamp;
  }

  public String getObjKey() {
    return objKey;
  }

  public String getCollKey() {
    return collKey;
  }

  public Operation getOperation() { 
    return operation; 
  }

  public String getVersionChecksum() { 
    return versionChecksum; 
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public void setTimestamp(Timestamp timestamp) {
    this.timestamp = timestamp;
  }

  public void setOperation(Operation operation) {
    this.operation = operation;
  }
}
