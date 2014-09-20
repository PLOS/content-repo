/*
 * Copyright (c) 2006-2014 by Public Library of Science
 * http://plos.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.plos.repo;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.plos.repo.models.Object;
import org.plos.repo.service.RepoService;
import org.springframework.beans.factory.config.SingletonBeanRegistry;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.net.URL;
import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.when;

public class CachingHeadersTest extends RepoBaseJerseyTest  {

  private final static String REPO_SVC_BEAN_NAME  = "repoService";
  private final static String BUCKET_NAME         = "plos-objstoreunittest-bucket1";
  private final static String KEY_NAME            = "keyname";
  private final static String REPROXY_HEADER_FILE = "reproxy-file";

  private static final DateTimeFormatter RFC1123_DATE_TIME_FORMATTER =
    DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'")
    .withZoneUTC();

  RepoService mockRepoService;
  DateTime    modifiedSinceDateTime;

  @Before
  public void setup() throws Exception {
    RepoBaseSpringTest.clearData(objectStore, sqlService);

    modifiedSinceDateTime = new DateTime(2014, 6, 25, 0, 0 ,0);
    mockRepoService = Mockito.mock(RepoService.class);
  }

  @Test
  public void testReadObjectNotFound() throws Exception {
    when(mockRepoService.getObject(anyString(), anyString(), anyInt())).thenReturn(null);
    registerObjectInSpring(mockRepoService);

    Response response = target("/objects/" + BUCKET_NAME)
                          .queryParam("key", KEY_NAME).queryParam("version", "0")
                          .request().get();

    assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
  }

  @Test
  public void testReadWithObjectModifiedBeforeIfModifiedSinceHeaderNoRepoxyHeaders() throws Exception {

    when(mockRepoService.getObject(anyString(), anyString(), anyInt()))
      .thenReturn(getObject(modifiedSinceDateTime.minusSeconds(1)));

    registerObjectInSpring(mockRepoService);

    Response response = target("/objects/" + BUCKET_NAME)
                          .queryParam("key", KEY_NAME).queryParam("version", "0")
                          .request()
                          .header("If-Modified-Since", RFC1123_DATE_TIME_FORMATTER.print(modifiedSinceDateTime))
                          .get();

    assertEquals(Status.NOT_MODIFIED.getStatusCode(), response.getStatus());
  }

  @Test
  public void testReadWithObjectModifiedEqualToIfModifiedSinceHeaderNoRepoxyHeaders() throws Exception {

    when(mockRepoService.getObject(anyString(), anyString(), anyInt()))
      .thenReturn(getObject(modifiedSinceDateTime));

    registerObjectInSpring(mockRepoService);

    Response response = target("/objects/" + BUCKET_NAME)
                          .queryParam("key", KEY_NAME).queryParam("version", "0")
                          .request()
                          .header("If-Modified-Since", RFC1123_DATE_TIME_FORMATTER.print(modifiedSinceDateTime))
                          .get();

    assertEquals(Status.NOT_MODIFIED.getStatusCode(), response.getStatus());
  }

  @Test
  public void testReadNoIfModifiedSinceHeaderNoRepoxyHeaders() throws Exception {

    when(mockRepoService.getObject(anyString(), anyString(), anyInt()))
      .thenReturn(getObject(modifiedSinceDateTime));

    registerObjectInSpring(mockRepoService);

    Response response = target("/objects/" + BUCKET_NAME)
                          .queryParam("key", KEY_NAME).queryParam("version", "0")
                          .request()
                          .get();

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }

  @Test
  public void testReadWithObjectModifiedAfterIfModifiedSinceHeaderNoRepoxyHeaders() throws Exception {

    when(mockRepoService.getObject(anyString(), anyString(), anyInt()))
      .thenReturn(getObject(modifiedSinceDateTime.plusSeconds(1)));

    registerObjectInSpring(mockRepoService);

    Response response = target("/objects/" + BUCKET_NAME)
                          .queryParam("key", KEY_NAME).queryParam("version", "0")
                          .request()
                          .header("If-Modified-Since", RFC1123_DATE_TIME_FORMATTER.print(modifiedSinceDateTime))
                          .get();

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }
  
  @Test
  public void testReadWithObjectModifiedBeforeIfModifiedSinceHeaderWithRepoxyHeaders() throws Exception {

    when(mockRepoService.getObject(anyString(), anyString(), anyInt()))
      .thenReturn(getObject(modifiedSinceDateTime.minusSeconds(1)));

    when(mockRepoService.serverSupportsReproxy())
      .thenReturn(true);

    URL[] urls = new URL[] {
      new URL("http", "192.168.1.1", "/dev1/123456.fid"),
      new URL("http", "192.168.1.1", "/dev5/123666.fid"),
      new URL("http", "192.168.1.3", "/dev5/789012.fid")
    };

    when(mockRepoService.getObjectReproxy(isA(org.plos.repo.models.Object.class)))
      .thenReturn(urls);

    registerObjectInSpring(mockRepoService);

    Response response = target("/objects/" + BUCKET_NAME)
                          .queryParam("key", KEY_NAME).queryParam("version", "0")
                          .request()
                          .header("If-Modified-Since", RFC1123_DATE_TIME_FORMATTER.print(modifiedSinceDateTime))
                          .header("X-Proxy-Capabilities", REPROXY_HEADER_FILE)
                          .get();

    assertEquals(Status.NOT_MODIFIED.getStatusCode(), response.getStatus());

    assertNotNull(response.getHeaderString("X-Reproxy-Cache-For"));

    String[] reproxyUrls = response.getHeaderString("X-Reproxy-URL").split(" ");
    assertEquals(3, reproxyUrls.length);

    // do sanity check on urls
    for (String url : reproxyUrls) {
      assertTrue( url.matches("^http://192.*fid$") );
    }
  }

  @Test
  public void testReadWithObjectModifiedAfterIfModifiedSinceHeaderWithRepoxyHeaders() throws Exception {

    when(mockRepoService.getObject(anyString(), anyString(), anyInt()))
      .thenReturn(getObject(modifiedSinceDateTime.plusSeconds(1)));

    when(mockRepoService.serverSupportsReproxy())
      .thenReturn(true);

    URL[] urls = new URL[] {
      new URL("http", "192.168.1.1", "/dev1/123456.fid"),
      new URL("http", "192.168.1.1", "/dev5/123666.fid"),
      new URL("http", "192.168.1.3", "/dev5/789012.fid")
    };

    when(mockRepoService.getObjectReproxy(isA(org.plos.repo.models.Object.class)))
      .thenReturn(urls);

    registerObjectInSpring(mockRepoService);

    Response response = target("/objects/" + BUCKET_NAME)
                          .queryParam("key", KEY_NAME).queryParam("version", "0")
                          .request()
                          .header("If-Modified-Since", RFC1123_DATE_TIME_FORMATTER.print(modifiedSinceDateTime))
                          .header("X-Proxy-Capabilities", REPROXY_HEADER_FILE)
                          .get();

    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    assertNotNull(response.getHeaderString("X-Reproxy-Cache-For"));

    String[] reproxyUrls = response.getHeaderString("X-Reproxy-URL").split(" ");
    assertEquals(3, reproxyUrls.length);

    // do sanity check on urls
    for (String url : reproxyUrls) {
      assertTrue( url.matches("^http://192.*fid$") );
    }
  }

  private void registerObjectInSpring(RepoService mock) {
    // do some magic to replace registered repoService singleton with mocked version.
    SingletonBeanRegistry beanRegistry = context.getBeanFactory();
    ((org.springframework.beans.factory.support.DefaultSingletonBeanRegistry)beanRegistry).destroySingleton(REPO_SVC_BEAN_NAME);
    beanRegistry.registerSingleton(REPO_SVC_BEAN_NAME, mock);
  }

  private org.plos.repo.models.Object getObject(DateTime datetime) {
    return new org.plos.repo.models.Object(
        Integer.valueOf(1),                         // id
        KEY_NAME,                                   // key name
        "checksum",                                 // checksum
        new Timestamp(datetime.toDate().getTime()), // timestamp
        "download-name",                            // download name
        "text/plain",                               // content type
        Long.valueOf(1),                            // size
        null,                                       // tag
        Integer.valueOf(1),                         // bucket id
        BUCKET_NAME,                                // bucket name
        Integer.valueOf(0),                         // version#
        org.plos.repo.models.Status.USED,
        new Timestamp(datetime.toDate().getTime())); // creation date time
  }
}
