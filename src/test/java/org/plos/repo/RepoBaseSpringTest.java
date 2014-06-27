package org.plos.repo;

import org.junit.runner.RunWith;
import org.plos.repo.models.*;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.RepoService;
import org.plos.repo.service.SqlService;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.inject.Inject;
import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestSpringConfig.class)
public abstract class RepoBaseSpringTest {

  @Inject
  protected RepoService repoService;

  @Inject
  protected ObjectStore objectStore;

  @Inject
  protected SqlService sqlService;

  public static void clearData(ObjectStore objectStore, SqlService sqlService) throws Exception {

    sqlService.getConnection();

    List<org.plos.repo.models.Object> objectList = sqlService.listObjects(null, null, null, true);

    for (org.plos.repo.models.Object object : objectList) {

      if (sqlService.deleteObject(object) == 0)
        throw new Exception("Object not deleted in DB");

      objectStore.deleteObject(object);
    }

    List<Bucket> bucketList = sqlService.listBuckets();

    for (Bucket bucket : bucketList) {
      sqlService.deleteBucket(bucket.bucketName);
      objectStore.deleteBucket(bucket);
    }

    // TODO: assert both refs are empty

    sqlService.transactionCommit();
  }

}