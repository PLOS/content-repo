package org.plos.repo;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.AfterClass;
import org.plos.repo.models.Bucket;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.SqlService;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.sql.SQLException;
import java.util.List;

public abstract class RepoBaseTest extends JerseyTest {

  protected SqlService sqlService;

  protected ObjectStore objectStore;

  private static AnnotationConfigApplicationContext context;

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

  public void clearData() throws SQLException {
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
