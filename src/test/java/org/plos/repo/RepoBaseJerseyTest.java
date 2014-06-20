package org.plos.repo;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.AfterClass;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.SqlService;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public abstract class RepoBaseJerseyTest extends JerseyTest {

  protected SqlService sqlService;

  protected ObjectStore objectStore;

  protected static AnnotationConfigApplicationContext context;

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
