package org.plos.repo.models;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.sql.Timestamp;
import java.util.EnumSet;

public class Asset {

  public enum Status {
    USED(0), DELETED(1);

    private final int value;

    private Status(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  public static final ImmutableMap<Integer, Status> STATUS_VALUES = Maps.uniqueIndex(EnumSet.allOf(Status.class),
      new Function<Status, Integer>() {
        @Override
        public Integer apply(Status status) {
          return status.getValue();
        }
      });

  public Integer id; // assigned by the db
  public String urls; // assigned by the backend storage (S3, Mogile, FS), space separated
  public String key; // what the user specifies
  public String checksum;  // of the file contents
  public Timestamp timestamp;   // created time
  public String downloadName;
  public String contentType;
  public Long size;
  public String tag;
  public Integer bucketId;
  public String bucketName;
  public Integer versionNumber;
  public Status status;

  public Asset(Integer id, String key, String checksum, Timestamp timestamp, String downloadName, String contentType, Long size, String urls, String tag, Integer bucketId, String bucketName, Integer versionNumber, Status status) {
    this.id = id;
    this.key = key;
    this.checksum = checksum;
    this.timestamp = timestamp;
    this.downloadName = downloadName;
    this.contentType = contentType;
    this.size = size;
    this.urls = urls;
    this.tag = tag;
    this.bucketId = bucketId;
    this.bucketName = bucketName;
    this.versionNumber = versionNumber;
    this.status = status;
  }

}
