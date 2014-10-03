package org.plos.repo.models.output;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
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
public class Collection {


  private String key; // what the user specifies
  @XmlJavaTypeAdapter(TimestampAdapter.class)
  private Timestamp timestamp;   // created time
  private Integer versionNumber;
  private String tag;
  @XmlJavaTypeAdapter(TimestampAdapter.class)
  private Timestamp creationDate;
  private String versionChecksum;
  private Status status;

  private List<Object> objects;

  private List<Collection> versions;


  public Collection() {
  }

  public Collection(org.plos.repo.models.Collection collection) {
    this.key = collection.getKey();
    this.timestamp = collection.getTimestamp();
    this.versionNumber = collection.getVersionNumber();
    this.tag = collection.getTag();
    this.creationDate = collection.getCreationDate();
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

  public Timestamp getTimestamp() {
    return timestamp;
  }

  public Integer getVersionNumber() {
    return versionNumber;
  }

  public List<Object> getObjects() {
    return objects;
  }

  public List<Collection> getVersions() {
    return versions;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setTimestamp(Timestamp timestamp) {
    this.timestamp = timestamp;
  }

  public void setVersionNumber(Integer versionNumber) {
    this.versionNumber = versionNumber;
  }

  public void setObjects(List<Object> objects) {
    this.objects = objects;
  }

  public void setVersions(List<Collection> versions) {
    this.versions = versions;
  }

  public String getTag() {
    return tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public Timestamp getCreationDate() {
    return creationDate;
  }

  public void setCreationDate(Timestamp creationDate) {
    this.creationDate = creationDate;
  }

  public String getVersionChecksum() {
    return versionChecksum;
  }

  public void setVersionChecksum(String versionChecksum) {
    this.versionChecksum = versionChecksum;
  }

  public boolean equals(Object o) {
    return EqualsBuilder.reflectionEquals(this, o);
  }

  @Override
  public int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
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
