package org.plos.repo.service;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.plos.repo.TestSpringConfig;
import org.plos.repo.models.*;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.inject.Inject;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestSpringConfig.class)
public class RepoServiceSpringTest {

  @Inject
  private RepoService repoService;

  @Inject
  private ObjectStore objectStore;

  @Inject
  private SqlService sqlService;

  @After
  public void afterMethods() throws SQLException {
    clearData();
  }

  public void clearData() throws SQLException {

    sqlService.getConnection();

    List<org.plos.repo.models.Object> objectList = sqlService.listAllObject();

    for (org.plos.repo.models.Object object : objectList) {
      sqlService.deleteObject(object);
      objectStore.deleteObject(object);
    }

    List<Bucket> bucketList = sqlService.listBuckets();

    for (Bucket bucket : bucketList) {
      sqlService.deleteBucket(bucket.bucketName);
      objectStore.deleteBucket(bucket);
    }

    sqlService.transactionCommit();
  }

  @Test
  public void repoServiceNotNull() {
    Assert.assertNotNull(repoService);
  }

  @Test
  public void connectionAtomicityTest() throws Exception {

    sqlService.getConnection();

    Assert.assertTrue(sqlService.getBucketId("bucket1") == null);

    sqlService.insertBucket(new Bucket("bucket1"));

    Assert.assertTrue(sqlService.getBucketId("bucket1") != null);

    sqlService.transactionCommit();

    sqlService.getConnection();
    Assert.assertTrue(sqlService.getBucketId("bucket1") != null);
    sqlService.releaseConnection();

  }

  @Test
  public void createBucketSuccessTest() throws Exception {

    try {
      repoService.createBucket("bucket1");
    } catch (RepoException e) {
      Assert.fail();
    }

    sqlService.getConnection();
    Assert.assertTrue(sqlService.getBucketId("bucket1") != null);
    sqlService.releaseConnection();
    Assert.assertTrue(objectStore.deleteBucket(new Bucket("bucket1")));
  }

  @Test
  public void createBucketRollbackStoreTest() throws Exception {

    ObjectStore spyObjectStore = Mockito.spy(objectStore);

    BDDMockito.willReturn(false).given(spyObjectStore).createBucket(Mockito.any(Bucket.class));

    Field objStoreField = RepoService.class.getDeclaredField("objectStore");
    objStoreField.setAccessible(true);
    objStoreField.set(repoService, spyObjectStore);

    try {
      repoService.createBucket("bucket1");
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ClientError);
      // TODO: check message
    }

    // check db state

    sqlService.getConnection();
    Assert.assertTrue(sqlService.getBucketId("bucket1") == null);
    sqlService.releaseConnection();
    Assert.assertFalse(objectStore.deleteBucket(new Bucket("bucket1")));
  }

  @Test
  public void createBucketRollbackDbTest() throws Exception {

    SqlService spySqlService = Mockito.spy(sqlService);

    BDDMockito.willReturn(false).given(spySqlService).insertBucket(Mockito.any(Bucket.class));

    Field sqlServiceField = RepoService.class.getDeclaredField("sqlService");
    sqlServiceField.setAccessible(true);
    sqlServiceField.set(repoService, spySqlService);

    try {
      repoService.createBucket("bucket1");
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ClientError);
    }

    // check db state

    sqlService.getConnection();
    Assert.assertTrue(sqlService.getBucketId("bucket1") == null);
    sqlService.releaseConnection();
    Assert.assertFalse(objectStore.deleteBucket(new Bucket("bucket1")));

  }
}
