package org.plos.repo.models;

import java.sql.Timestamp;

public class Asset {

  public Integer id; // assigned by the db
  public String url; // assigned by the backend storage (S3, Mogile, FS)
  public String key; // what the user specifies
  public String checksum;  // of the file contents
  public Timestamp timestamp;   // created time
  public String downloadName;
  public String contentType;
  public Long size;
  public String tag;
  public Integer bucketId;
  public String bucketName;

  public Asset(Integer id, String key, String checksum, Timestamp timestamp, String downloadName, String contentType, Long size, String url, String tag, Integer bucketId, String bucketName) {

    this.id = id;
    this.key = key;
    this.checksum = checksum;
    this.timestamp = timestamp;
    this.downloadName = downloadName;
    this.contentType = contentType;
    this.size = size;
    this.url = url;
    this.tag = tag;
    this.bucketId = bucketId;
    this.bucketName = bucketName;
  }

}
