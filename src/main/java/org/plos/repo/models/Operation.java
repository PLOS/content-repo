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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.EnumSet;

/**
 * Represents the operations in audit events
 */
public enum Operation {

  CREATE_BUCKET("create-bucket"), 
  CREATE_OBJECT("create-object"), 
  CREATE_COLLECTION("create-collection"),
  UPDATE_OBJECT("update-object"),
  UPDATE_COLLECTION("update-collection"),
  DELETE_BUCKET("delete-bucket"), 
  DELETE_OBJECT("delete-object"), 
  DELETE_COLLECTION("delete-collection") ,
  PURGE_OBJECT("purge-object");
  
  private final String value;

  private Operation(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  public static final ImmutableMap<String, Operation> OPERATION_VALUES = Maps.uniqueIndex(EnumSet.allOf(Operation.class),
          new Function<Operation, String>() {
            @Override
            public String apply(Operation operation) {
              return operation.getValue();
            }
          });

}
