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
