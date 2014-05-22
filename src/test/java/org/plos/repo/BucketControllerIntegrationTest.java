package org.plos.repo;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class BucketControllerIntegrationTest {

  private static final String URL = "http://localhost:8080/";

  @Test
  public void testIfAppIsUp() throws IOException {

    //given
    HttpClient client = new DefaultHttpClient();
    HttpGet httpget = new HttpGet(URL);

    //when
    HttpResponse response = client.execute(httpget);

    //then
    assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_OK);
  }

}
