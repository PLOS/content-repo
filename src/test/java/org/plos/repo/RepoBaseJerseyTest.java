package org.plos.repo;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.AfterClass;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.RepoException;
import org.plos.repo.service.SqlService;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import javax.ws.rs.core.Response;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class RepoBaseJerseyTest extends JerseyTest {

  protected SqlService sqlService;

  protected ObjectStore objectStore;

  protected static AnnotationConfigApplicationContext context;

  protected Gson gson = new Gson();

  protected void assertRepoError(Response response, Response.Status httpStatus, RepoException.Type repoError) {

    JsonObject responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonObject();

    assertEquals(response.getStatus(), httpStatus.getStatusCode());

    assertEquals(responseObj.get("repoErrorCode").getAsInt(), repoError.getValue());
    assertTrue(responseObj.get("message").getAsString().equals(repoError.getMessage()));
  }

  @Override
  protected javax.ws.rs.core.Application configure() {

    context = new AnnotationConfigApplicationContext(TestSpringConfig.class);

    ResourceConfig config = new JerseyApplication().property("contextConfig", context);

    sqlService = context.getBean(SqlService.class);
    objectStore = context.getBean(ObjectStore.class);

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

}
