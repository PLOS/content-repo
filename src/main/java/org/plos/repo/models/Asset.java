package org.plos.repo.models;

import java.util.Date;

public class Asset {

  public String id; // assigned by the db
  public String url; // assigned by the backend storage (S3, Mogile, FS)
  public String key; // what the user specifies
  public String checksum;  // of the file contents
  public Date timestamp;   // created time

  // bucket

}
