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

package org.plos.repo;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.plos.repo.models.RepoObject;
import org.plos.repo.models.input.ElementFilter;
import org.plos.repo.service.RepoService;
import org.springframework.beans.factory.config.SingletonBeanRegistry;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.net.URL;
import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.when;

public class CachingHeadersTest extends RepoBaseJerseyTest {

  private final static String REPO_SVC_BEAN_NAME = "repoService";
  private final static String BUCKET_NAME = "plos-objstoreunittest-bucket1";
  private final static String KEY_NAME = "keyname";
  private final static String REPROXY_HEADER_FILE = "reproxy-file";

  private static final DateTimeFormatter RFC1123_DATE_TIME_FORMATTER =
      DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'")
          .withZoneUTC();

  RepoService mockRepoService;
  DateTime modifiedSinceDateTime;

  @Before
  public void setup() throws Exception {
    modifiedSinceDateTime = new DateTime(2014, 6, 25, 0, 0, 0);
    mockRepoService = Mockito.mock(RepoService.class);
  }

  @Test
  public void testReadObjectNotFound() throws Exception {
    when(mockRepoService.getObject(anyString(), anyString(), any(ElementFilter.class))).thenReturn(null);
    registerObjectInSpring(mockRepoService);

    Response response = target("/objects/" + BUCKET_NAME)
        .queryParam("key", KEY_NAME).queryParam("version", "0")
        .request().get();

    assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
  }

  @Test
  public void testReadWithObjectModifiedBeforeIfModifiedSinceHeaderNoRepoxyHeaders() throws Exception {
    when(mockRepoService.getObject(anyString(), anyString(), any(ElementFilter.class)))
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
    when(mockRepoService.getObject(anyString(), anyString(), any(ElementFilter.class)))
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
    when(mockRepoService.getObject(anyString(), anyString(), any(ElementFilter.class)))
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
    when(mockRepoService.getObject(anyString(), anyString(), any(ElementFilter.class)))
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
    when(mockRepoService.getObject(anyString(), anyString(), any(ElementFilter.class)))
        .thenReturn(getObject(modifiedSinceDateTime.minusSeconds(1)));

    when(mockRepoService.serverSupportsReproxy())
        .thenReturn(true);

    URL[] urls = new URL[]{
        new URL("http", "192.168.1.1", "/dev1/123456.fid"),
        new URL("http", "192.168.1.1", "/dev5/123666.fid"),
        new URL("http", "192.168.1.3", "/dev5/789012.fid")
    };

    when(mockRepoService.getObjectReproxy(isA(RepoObject.class)))
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
      assertTrue(url.matches("^http://192.*fid$"));
    }
  }

  @Test
  public void testReadWithObjectModifiedAfterIfModifiedSinceHeaderWithRepoxyHeaders() throws Exception {
    when(mockRepoService.getObject(anyString(), anyString(), any(ElementFilter.class)))
        .thenReturn(getObject(modifiedSinceDateTime.plusSeconds(1)));

    when(mockRepoService.serverSupportsReproxy())
        .thenReturn(true);

    URL[] urls = new URL[]{
        new URL("http", "192.168.1.1", "/dev1/123456.fid"),
        new URL("http", "192.168.1.1", "/dev5/123666.fid"),
        new URL("http", "192.168.1.3", "/dev5/789012.fid")
    };

    when(mockRepoService.getObjectReproxy(isA(RepoObject.class)))
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
      assertTrue(url.matches("^http://192.*fid$"));
    }
  }

  private void registerObjectInSpring(RepoService mock) {
    // do some magic to replace registered repoService singleton with mocked version.
    SingletonBeanRegistry beanRegistry = context.getBeanFactory();
    ((org.springframework.beans.factory.support.DefaultSingletonBeanRegistry) beanRegistry).destroySingleton(REPO_SVC_BEAN_NAME);
    beanRegistry.registerSingleton(REPO_SVC_BEAN_NAME, mock);
  }

  private RepoObject getObject(DateTime datetime) {
    RepoObject repoObject = new RepoObject(KEY_NAME, 1, BUCKET_NAME, org.plos.repo.models.Status.USED);
    repoObject.setId(1);
    repoObject.setChecksum("checksum");
    repoObject.setTimestamp(new Timestamp(datetime.toDate().getTime()));
    repoObject.setDownloadName("download-name");
    repoObject.setContentType("text/plain");
    repoObject.setSize((long) 1);
    repoObject.setVersionNumber(0);
    repoObject.setCreationDate(new Timestamp(datetime.toDate().getTime()));
    return repoObject;
  }

}
