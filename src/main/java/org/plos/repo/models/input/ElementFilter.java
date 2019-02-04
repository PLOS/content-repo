/*
 * Copyright (c) 2014-2019 Public Library of Science
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

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
