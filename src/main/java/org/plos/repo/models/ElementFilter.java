package org.plos.repo.models;


import javax.ws.rs.QueryParam;

public class ElementFilter {

  @QueryParam("version")
  private Integer version;

  @QueryParam("tag")
  private String tag;

  @QueryParam("versionChecksum")
  private Integer versionChecksum;

  public ElementFilter(){

  }

  public ElementFilter(Integer version, String tag, Integer versionChecksum){
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

  public Integer getVersionChecksum() {
    return versionChecksum;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public void setVersionChecksum(Integer versionChecksum) {
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
