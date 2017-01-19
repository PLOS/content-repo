/*
 * Copyright (c) 2017 Public Library of Science
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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.RepoObject;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.RepoException;
import org.plos.repo.service.SqlService;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import javax.sql.DataSource;
import javax.ws.rs.core.Response;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class RepoBaseJerseyTest extends JerseyTest{

  protected ObjectStore objectStore;

  protected SqlService sqlService;

  protected DataSource dataSource;

  protected Gson gson = new Gson();

  protected static AnnotationConfigApplicationContext context;

  public RepoBaseJerseyTest(){
    super();
    sqlService = context.getBean(SqlService.class);
    objectStore = context.getBean(ObjectStore.class);
    dataSource = context.getBean(DataSource.class);
  }

  protected void assertRepoError(Response response, Response.Status httpStatus, RepoException.Type repoError) {
    JsonObject responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonObject();

    assertEquals(httpStatus.getStatusCode(), response.getStatus());

    assertEquals(responseObj.get("repoErrorCode").getAsInt(), repoError.getValue());
    assertTrue(responseObj.get("message").getAsString().equals(repoError.getMessage()));
  }

  @Override
  protected javax.ws.rs.core.Application configure() {
    context = new AnnotationConfigApplicationContext(TestSpringConfig.class);
    ResourceConfig config = new JerseyApplication().property("contextConfig", context);

    return config;
  }

  @AfterClass
  public static void afterClass() throws Exception {
    // since we are manually creating the beans above, we need to close the context explicitly for PreDestory
    context.close();
  }

  @Override
  protected void configureClient(ClientConfig config) {
    config.register(MultiPartFeature.class);
  }

  /**
   * Clean de data base before to run each test
   * @throws Exception
   */
  @Before
  public void clearData() throws Exception {
    try(Connection connection = dataSource.getConnection();
        Statement st = connection.createStatement();) {

      sqlService.getConnection();
      List<RepoObject> repoObjectList = sqlService.listObjects(null, null, null, true, true, null);
      for (RepoObject repoObject : repoObjectList) {
        objectStore.deleteObject(repoObject);
      }

      List<Bucket> bucketList = sqlService.listBuckets();
      for (Bucket bucket : bucketList) {
        objectStore.deleteBucket(bucket);
      }

      st.executeUpdate("delete from collectionObject");
      st.executeUpdate("delete from collections");
      st.executeUpdate("delete from objects");
      st.executeUpdate("delete from buckets");
      st.executeUpdate("delete from audit");
    }
  }

}
