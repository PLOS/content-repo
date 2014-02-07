package org.plos.repo.models;

import java.sql.Timestamp;

public class Asset {

  public int id; // assigned by the db
  public String url; // assigned by the backend storage (S3, Mogile, FS)
  public String key; // what the user specifies
  public String checksum;  // of the file contents
  public Timestamp timestamp;   // created time
  public String downloadName;
  public String contentType;
  public long size;
  public String tag;
  public Integer bucketId;
  public String bucketName;
}
