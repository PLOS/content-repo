package org.plos.repo.models;


import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class TimestampFormatter {

  private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd-kk.mm.ss";

  public String getFormattedTimestamp(Timestamp timestamp){

    DateFormat df = new SimpleDateFormat(TIMESTAMP_FORMAT);
    return df.format(timestamp);

  }

}
