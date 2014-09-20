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
