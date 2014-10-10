package org.plos.repo.models.output;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.plos.repo.models.Status;
import org.plos.repo.models.TimestampAdapter;
import org.plos.repo.models.TimestampFormatter;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.sql.Timestamp;
import java.util.List;

/**
 * Collection to be return to the client
 */
@XmlRootElement
public class Collection {

  private String key; // what the user specifies
  private String timestamp;   // created time
  private Integer versionNumber;
  private String tag;
  private String creationDate;
  private String versionChecksum;
  private Status status;
  private List<Object> objects;

  private TimestampFormatter timestampFormatter;

  public Collection() {
    this.timestampFormatter = new TimestampFormatter();
  }

  public Collection(org.plos.repo.models.Collection collection) {
    this.timestampFormatter = new TimestampFormatter();
    this.key = collection.getKey();
    this.timestamp = timestampFormatter.getFormattedTimestamp(collection.getTimestamp());
    this.versionNumber = collection.getVersionNumber();
    this.tag = collection.getTag();
    this.creationDate = timestampFormatter.getFormattedTimestamp(collection.getCreationDate());
    this.versionChecksum = collection.getVersionChecksum();
    this.status = collection.getStatus();

    if(collection.getObjects()!=null && collection.getObjects().size() >0){
      this.objects = Lists.newArrayList(Iterables.transform(collection.getObjects(), Object.typeFunction()));
    }

  }

  public void addObjects(List<Object> objects){

    this.objects = objects;
  }

  public String getKey() {
    return key;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public Integer getVersionNumber() {
    return versionNumber;
  }

  public List<Object> getObjects() {
    return objects;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setTimestamp(Timestamp timestamp) {
    this.timestamp = timestampFormatter.getFormattedTimestamp(timestamp);
  }

  public void setVersionNumber(Integer versionNumber) {
    this.versionNumber = versionNumber;
  }

  public void setObjects(List<Object> objects) {
    this.objects = objects;
  }

  public String getTag() {
    return tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public String getCreationDate() {
    return creationDate;
  }

  public void setCreationDate(Timestamp creationDate) {
    this.creationDate = timestampFormatter.getFormattedTimestamp(creationDate);
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  public void setCreationDate(String creationDate) {
    this.creationDate = creationDate;
  }

  public String getVersionChecksum() {
    return versionChecksum;
  }

  public void setVersionChecksum(String versionChecksum) {
    this.versionChecksum = versionChecksum;
  }

  @Override
  public int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
  }

  @Override
  public boolean equals(java.lang.Object obj) {
    return EqualsBuilder.reflectionEquals(this, obj);
  }

  public static Function<org.plos.repo.models.Collection, Collection> typeFunction() {
    return new Function<org.plos.repo.models.Collection, Collection>() {

      @Override
      public Collection apply(org.plos.repo.models.Collection collection) {
        return new Collection(collection);
      }

    };
  }

}
