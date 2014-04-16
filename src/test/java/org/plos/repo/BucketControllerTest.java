package org.plos.repo;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;
import org.plos.repo.rest.BucketController;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class BucketControllerTest extends JerseyTest {

  @Override
  protected javax.ws.rs.core.Application configure() {

    ApplicationContext context = new AnnotationConfigApplicationContext(TestConfig.class);
    final ResourceConfig config = new ResourceConfig().property("contextConfig", context);
    config.register(BucketController.class);
    return config;

//    ResourceConfig rc = new ResourceConfig().register(BucketController.class)
//        .register(SpringLifecycleListener.class)
//        .register(RequestContextFilter.class)
//        .register(TestConfig.class)
//        .property("contextClass", "org.springframework.web.context.support.AnnotationConfigWebApplicationContext")
//        .property("contextConfigLocation", "org.plos.repo.config.Config");
//    return rc;

//    return new Application();

//    return new ResourceConfig().register(SpringLifecycleListener.class);

//    return new ResourceConfig(BucketController.class);

//    return new LowLevelAppDescriptor.Builder(BucketController.class)
//        .clientConfig( new DefaultClientConfig())
//        .build();
  }


  @Test
  public void testControllerCrud() {

    // CREATE

    String responseString = target("/buckets").request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    Assert.assertEquals(responseString, "[]");

    Form form = new Form().param("name", "plos-bucketunittest-bucket1");
    Response response = target("/buckets").request(MediaType.APPLICATION_JSON_TYPE).post(Entity.form(form));

    Assert.assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());

  }

}
