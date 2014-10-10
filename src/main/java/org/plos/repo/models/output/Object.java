package org.plos.repo.models.output;

import com.google.common.base.Function;
import org.plos.repo.models.Status;
import org.plos.repo.models.TimestampAdapter;
import org.plos.repo.models.TimestampFormatter;
import org.springframework.context.ApplicationContext;

import javax.inject.Inject;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.sql.Timestamp;

/**
 * Collection to be return to the client
 */
@XmlRootElement
public class Object {

  private String key; // what the user specifies
  private String checksum;  // of the file contents

  private String timestamp;   // last modification time
  private String downloadName;
  private String contentType;
  private Long size;
  private String tag;
  private Integer versionNumber;
  private Status status;
  private String creationDate;
  private String versionChecksum;
  private String reproxyURL;

  private TimestampFormatter timestampFormatter;

  public Object() {
    this.timestampFormatter = new TimestampFormatter();
  }

  public Object(org.plos.repo.models.Object object) {

    this.timestampFormatter = new TimestampFormatter();
    this.key = object.getKey();
    this.checksum = object.getChecksum();
    this.timestamp = timestampFormatter.getFormattedTimestamp(object.getTimestamp());
    this.downloadName = object.getDownloadName();
    this.contentType = object.getContentType();
    this.size = object.getSize();
    this.tag = object.getTag();
    this.versionNumber = object.getVersionNumber();
    this.status = object.getStatus();
    this.creationDate = timestampFormatter.getFormattedTimestamp(object.getCreationDate());
    this.versionChecksum = object.getVersionChecksum();

    if (object.getReproxyURL() != null ) {
      this.reproxyURL = object.getReproxyURL().toString();
    }

  }

  public String getKey() {
    return key;
  }

  public String getChecksum() {
    return checksum;
  }

  public String getTimestamp() {
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

  public String getCreationDate() {
    return creationDate;
  }

  public String getVersionChecksum() {
    return versionChecksum;
  }

  public String getReproxyURL() {
    return reproxyURL;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setChecksum(String checksum) {
    this.checksum = checksum;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  public void setTimestamp(Timestamp timestamp) {
    this.timestamp = timestampFormatter.getFormattedTimestamp(timestamp);
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

  public void setCreationDate(String creationDate) {
    this.creationDate = creationDate;
  }

  public void setCreationDate(Timestamp creationDate) {
    this.creationDate = timestampFormatter.getFormattedTimestamp(creationDate);
  }

  public void setVersionChecksum(String versionChecksum) {
    this.versionChecksum = versionChecksum;
  }

  public void setReproxyURL(String reproxyURL) {
    this.reproxyURL = reproxyURL;
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
