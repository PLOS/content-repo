package org.plos.repo;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;
import org.plos.repo.rest.BucketController;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;

public class JerseyBucketTest extends JerseyTest {

  @Override
  protected Application configure() {

//    return new ResourceConfig().register(SpringLifecycleListener.class);

    return new ResourceConfig(BucketController.class);

//    return new LowLevelAppDescriptor.Builder(BucketController.class)
//        .clientConfig( new DefaultClientConfig())
//        .build();
  }

  @Test
  public void testControllerCrud() {
    String response = target("/buckets").request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    Assert.assertEquals(response, "[]");
  }
}
