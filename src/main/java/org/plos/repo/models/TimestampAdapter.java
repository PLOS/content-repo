package org.plos.repo.models;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.sql.Timestamp;
import java.util.Date;

public class TimestampAdapter extends XmlAdapter<Date, Timestamp> {
  public Date marshal(Timestamp v) {
    return new Date(v.getTime());
  }
  public Timestamp unmarshal(Date v) {
    return new Timestamp(v.getTime());
  }
}
