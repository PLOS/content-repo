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
 * Represents the status of an model object.
 */
public enum Status {

  USED(0), DELETED(1);

  private final int value;

  private Status(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static final ImmutableMap<Integer, Status> STATUS_VALUES = Maps.uniqueIndex(EnumSet.allOf(Status.class),
      new Function<Status, Integer>() {
        @Override
        public Integer apply(Status status) {
          return status.getValue();
        }
      });

}
