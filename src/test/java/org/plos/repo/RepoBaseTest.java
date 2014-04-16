package org.plos.repo;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spring.scope.RequestContextFilter;
import org.glassfish.jersey.test.JerseyTest;
import org.plos.repo.models.*;
import org.plos.repo.rest.BucketController;
import org.plos.repo.rest.ObjectController;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.SqlService;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.List;

public abstract class RepoBaseTest extends JerseyTest {

  protected SqlService sqlService;

  protected ObjectStore objectStore;

  @Override
  protected javax.ws.rs.core.Application configure() {

    ApplicationContext context = new AnnotationConfigApplicationContext(TestConfig.class);
    ResourceConfig config = new ResourceConfig().property("contextConfig", context);
    config.register(BucketController.class);
    config.register(ObjectController.class);
    config.register(MultiPartFeature.class);
    config.register(RequestContextFilter.class);

    sqlService = context.getBean(SqlService.class);
    objectStore = context.getBean(ObjectStore.class);

    return config;
  }

  public void clearData() {
    List<org.plos.repo.models.Object> objectList = sqlService.listAllObject();

    for (org.plos.repo.models.Object object : objectList) {
      //sqlService.markObjectDeleted(object.key, object.checksum, object.bucketName, object.versionNumber);
      int delD = sqlService.deleteObject(object);
      boolean delS = objectStore.deleteObject(object);
    }

    List<Bucket> bucketList = sqlService.listBuckets();

    for (Bucket bucket : bucketList) {
      sqlService.deleteBucket(bucket.bucketName);
      objectStore.deleteBucket(bucket);
    }
  }

  @Override
  protected void configureClient(ClientConfig config) {
    config.register(MultiPartFeature.class);
  }

}
