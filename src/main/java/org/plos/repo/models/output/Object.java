package org.plos.repo.models.output;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.plos.repo.models.Status;
import org.plos.repo.models.TimestampAdapter;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.sql.Timestamp;
import java.util.List;

/**
 * Collection to be return to the client
 */
@XmlRootElement
public class Object {

  private String key; // what the user specifies
  private String checksum;  // of the file contents

  @XmlJavaTypeAdapter(TimestampAdapter.class)
  private Timestamp timestamp;   // last modification time
  private String downloadName;
  private String contentType;
  private Long size;
  private String tag;
  private Integer versionNumber;
  private Status status;
  private Timestamp creationDate;
  private String versionChecksum;

  public Object() {
  }

  public Object(org.plos.repo.models.Object object) {

    this.key = object.getKey();
    this.checksum = object.getChecksum();
    this.timestamp = object.getTimestamp();
    this.downloadName = object.getDownloadName();
    this.contentType = object.getContentType();
    this.size = object.getSize();
    this.tag = object.getTag();
    this.versionNumber = object.getVersionNumber();
    this.status = object.getStatus();
    this.creationDate = object.getCreationDate();
    this.versionChecksum = object.getVersionChecksum();

  }

  public String getKey() {
    return key;
  }

  public String getChecksum() {
    return checksum;
  }

  public Timestamp getTimestamp() {
    return timestamp;
  }

  public String getDownloadName() {
    return downloadName;
  }

  public String getContentType() {
    return contentType;
  }

  public Long getSize() {
    return size;
  }

  public String getTag() {
    return tag;
  }

  public Integer getVersionNumber() {
    return versionNumber;
  }

  public Status getStatus() {
    return status;
  }

  public Timestamp getCreationDate() {
    return creationDate;
  }

  public String getVersionChecksum() {
    return versionChecksum;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setChecksum(String checksum) {
    this.checksum = checksum;
  }

  public void setTimestamp(Timestamp timestamp) {
    this.timestamp = timestamp;
  }

  public void setDownloadName(String downloadName) {
    this.downloadName = downloadName;
  }

  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

  public void setSize(Long size) {
    this.size = size;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public void setVersionNumber(Integer versionNumber) {
    this.versionNumber = versionNumber;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public void setCreationDate(Timestamp creationDate) {
    this.creationDate = creationDate;
  }

  public void setVersionChecksum(String versionChecksum) {
    this.versionChecksum = versionChecksum;
  }

  public static Function<org.plos.repo.models.Object, Object> typeFunction() {
    return new Function<org.plos.repo.models.Object, Object>() {

      @Override
      public Object apply(org.plos.repo.models.Object object) {
        return new Object(object);
      }

    };
  }

}
