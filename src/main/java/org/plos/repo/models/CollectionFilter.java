package org.plos.repo.models;


import javax.ws.rs.QueryParam;

public class CollectionFilter {

  @QueryParam("version")
  private Integer version;

  @QueryParam("tag")
  private String tag;

  @QueryParam("versionChecksum")
  private String versionChecksum;

  public CollectionFilter(){

  }

  public CollectionFilter(Integer version, String tag, String versionChecksum){
    this.version = version;
    this.tag = tag;
    this.versionChecksum = versionChecksum;
  }

  public Integer getVersion() {
    return version;
  }

  public String getTag() {
    return tag;
  }

  public String getVersionChecksum() {
    return versionChecksum;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public void setVersionChecksum(String versionChecksum) {
    this.versionChecksum = versionChecksum;
  }

  public Boolean isEmpty(){
    return version == null && tag == null && versionChecksum == null;
  }

  @Override
  public String toString() {
    return "CollectionFilter{" +
        "version=" + version +
        ", tag='" + tag + '\'' +
        ", versionChecksum='" + versionChecksum + '\'' +
        '}';
  }
}
