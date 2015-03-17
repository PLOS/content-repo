package org.plos.repo.models.input;


import javax.ws.rs.QueryParam;

public class ElementFilter {

  @QueryParam("version")
  private Integer version;

  @QueryParam("tag")
  private String tag;

  @QueryParam("uuid")
  private String uuid;

  public ElementFilter() {
  }

  public ElementFilter(Integer version, String tag, String uuid) {
    this.version = version;
    this.tag = tag;
    this.uuid = uuid;
  }

  public Integer getVersion() {
    return version;
  }

  public String getTag() {
    return tag;
  }

  public String getUuid() {
    return uuid;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public Boolean isEmpty() {
    return version == null && tag == null && uuid == null;
  }

  @Override
  public String toString() {
    return "CollectionFilter{" +
        "version=" + version +
        ", tag='" + tag + '\'' +
        ", uuid='" + uuid + '\'' +
        '}';
  }

}
