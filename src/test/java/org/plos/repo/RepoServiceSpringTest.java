package org.plos.repo;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.plos.repo.TestSpringConfig;
import org.plos.repo.models.*;
import org.plos.repo.models.Object;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.RepoException;
import org.plos.repo.service.RepoService;
import org.plos.repo.service.SqlService;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.inject.Inject;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
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


  private final Bucket bucket1 = new Bucket("bucket1");

  @Before
  public void beforeMethods() throws Exception {

    // reset the internal beans since their mocks/spies retain between tests

    Field objStoreField = RepoService.class.getDeclaredField("objectStore");
    objStoreField.setAccessible(true);
    objStoreField.set(repoService, objectStore);

    Field sqlServiceField = RepoService.class.getDeclaredField("sqlService");
    sqlServiceField.setAccessible(true);
    sqlServiceField.set(repoService, sqlService);

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

    // TODO: assert both refs are empty

    sqlService.transactionCommit();
  }

  @Test
  public void repoServiceNotNull() {
    Assert.assertNotNull(repoService);
  }

  @Test
  public void listBuckets() throws Exception {
    Assert.assertTrue(repoService.listBuckets().size() == 0);
    repoService.createBucket(bucket1.bucketName);
    Assert.assertTrue(repoService.listBuckets().size() == 1);
  }

  @Test
  public void connectionAtomicityTest() throws Exception {

    sqlService.getConnection();

    Assert.assertTrue(sqlService.getBucketId(bucket1.bucketName) == null);

    sqlService.insertBucket(bucket1);

    Assert.assertTrue(sqlService.getBucketId(bucket1.bucketName) != null);

    sqlService.transactionCommit();

    sqlService.getConnection();
    Assert.assertTrue(sqlService.getBucketId(bucket1.bucketName) != null);
    sqlService.releaseConnection();

  }

  @Test
  public void createBucket() throws Exception {

    try {
      repoService.createBucket(bucket1.bucketName);
    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

    sqlService.getConnection();
    Assert.assertTrue(sqlService.getBucketId(bucket1.bucketName) != null);
    sqlService.releaseConnection();
    Assert.assertTrue(objectStore.bucketExists(bucket1));
  }

  @Test
  public void createBucketErrorInObjStore() throws Exception {

    ObjectStore spyObjectStore = Mockito.spy(objectStore);

    BDDMockito.willReturn(false).given(spyObjectStore).createBucket(Mockito.any(Bucket.class));

    Field objStoreField = RepoService.class.getDeclaredField("objectStore");
    objStoreField.setAccessible(true);
    objStoreField.set(repoService, spyObjectStore);

    try {
      repoService.createBucket(bucket1.bucketName);
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ClientError);
      Assert.assertTrue(e.getMessage().startsWith("Unable to create bucket in object store"));
    }

    // check db state

    sqlService.getConnection();
    Assert.assertTrue(sqlService.getBucketId(bucket1.bucketName) == null);
    sqlService.releaseConnection();
    Assert.assertFalse(objectStore.bucketExists(bucket1));
  }

  @Test
  public void createBucketErrorInDb() throws Exception {

    SqlService spySqlService = Mockito.spy(sqlService);

    BDDMockito.willReturn(false).given(spySqlService).insertBucket(Mockito.any(Bucket.class));

    Field sqlServiceField = RepoService.class.getDeclaredField("sqlService");
    sqlServiceField.setAccessible(true);
    sqlServiceField.set(repoService, spySqlService);

    try {
      repoService.createBucket(bucket1.bucketName);
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ClientError);
      Assert.assertTrue(e.getMessage().startsWith("Unable to create bucket in database"));
    }

    // check db state

    sqlService.getConnection();
    Assert.assertTrue(sqlService.getBucketId(bucket1.bucketName) == null);
    sqlService.releaseConnection();
    Assert.assertFalse(objectStore.bucketExists(bucket1));

  }

  @Test
  public void deleteBucket() throws Exception {

    repoService.createBucket(bucket1.bucketName);

    try {
      repoService.deleteBucket(bucket1.bucketName);
    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

    Assert.assertTrue(repoService.listBuckets().size() == 0);

    sqlService.getConnection();
    Assert.assertTrue(sqlService.getBucketId(bucket1.bucketName) == null);
    sqlService.releaseConnection();
    Assert.assertFalse(objectStore.bucketExists(bucket1));

  }

  @Test
  public void deleteBucketNotFoundInObjStore() throws Exception {

    ObjectStore spyObjectStore = Mockito.spy(objectStore);

    BDDMockito.willReturn(false).given(spyObjectStore).bucketExists(Mockito.any(Bucket.class));

    Field objStoreField = RepoService.class.getDeclaredField("objectStore");
    objStoreField.setAccessible(true);
    objStoreField.set(repoService, spyObjectStore);

    try {
      repoService.deleteBucket(bucket1.bucketName);
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ItemNotFound);
      Assert.assertTrue(e.getMessage().startsWith("Bucket not found in database"));
    }

  }

  @Test
  public void createNewObject() throws Exception {

    repoService.createBucket(bucket1.bucketName);

    try {
      repoService.createNewObject("key1", bucket1.bucketName, "text!", "dlName", new Timestamp(new Date().getTime()), IOUtils.toInputStream("data1"));
    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

    // check state

    sqlService.getConnection();

    Object objFromDb = sqlService.getObject(bucket1.bucketName, "key1");

    Assert.assertTrue(objFromDb.key.equals("key1"));
    sqlService.releaseConnection();
    Assert.assertTrue(objectStore.objectExists(objFromDb));

    Assert.assertTrue(IOUtils.toString(objectStore.getInputStream(objFromDb)).equals("data1"));


    Assert.assertTrue(repoService.getObjectVersions(objFromDb).size() == 1);
    Assert.assertTrue(repoService.getObjectContentType(objFromDb).equals("text!"));
    Assert.assertTrue(repoService.getObjectExportFileName(objFromDb).equals("dlName"));
  }

  @Test
  public void createNewObjectUploadError() throws Exception {

    repoService.createBucket(bucket1.bucketName);

    ObjectStore spyObjectStore = Mockito.spy(objectStore);

    BDDMockito.willReturn(false).given(spyObjectStore).saveUploadedObject(Mockito.any(Bucket.class), Mockito.any(ObjectStore.UploadInfo.class), Mockito.any(Object.class));

    Field objStoreField = RepoService.class.getDeclaredField("objectStore");
    objStoreField.setAccessible(true);
    objStoreField.set(repoService, spyObjectStore);

    try {
      repoService.createNewObject("key1", bucket1.bucketName, null, null, new Timestamp(new Date().getTime()), IOUtils.toInputStream("data1"));
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ServerError);
      Assert.assertTrue(e.getMessage().startsWith("Error saving content to object store"));
    }

    // check state

    sqlService.getConnection();
    Object objFromDb = sqlService.getObject(bucket1.bucketName, "key1");
    Assert.assertTrue(objFromDb == null);
    sqlService.releaseConnection();
    Assert.assertFalse(objectStore.objectExists(new Object(null, null, "cbcc2ff6a0894e6e7f9a1a6a6a36b68fb36aa151", null, null, null, 0l, null, null, bucket1.bucketName, null, null)));
  }

  @Test
  public void createNewObjectRollback() throws Exception {

    repoService.createBucket(bucket1.bucketName);

    SqlService spySqlService = Mockito.spy(sqlService);

    BDDMockito.willReturn(0).given(spySqlService).insertObject(Mockito.any(Object.class));

    Field sqlServiceField = RepoService.class.getDeclaredField("sqlService");
    sqlServiceField.setAccessible(true);
    sqlServiceField.set(repoService, spySqlService);

    try {
      repoService.createNewObject("key1", bucket1.bucketName, null, null, new Timestamp(new Date().getTime()), IOUtils.toInputStream("data1"));
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ServerError);
      Assert.assertTrue(e.getMessage().startsWith("Error saving content to database"));
    }

    // check state

    sqlService.getConnection();
    Object objFromDb = sqlService.getObject(bucket1.bucketName, "key1");
    Assert.assertTrue(objFromDb == null);
    sqlService.releaseConnection();
    Assert.assertTrue(objectStore.objectExists(new Object(null, null, "cbcc2ff6a0894e6e7f9a1a6a6a36b68fb36aa151", null, null, null, 0l, null, null, bucket1.bucketName, null, null)));  // since we do not delete object data
  }

  @Test
  public void createObjectVersion() throws Exception {

    repoService.createBucket(bucket1.bucketName);

    try {
      Object newObj = repoService.createNewObject("key1", bucket1.bucketName, "first content type", "first download name", new Timestamp(new Date().getTime()), IOUtils.toInputStream("data1"));
      repoService.updateObject(bucket1.bucketName, "new content type", "new download name", new Timestamp(new Date().getTime()), IOUtils.toInputStream("data2"), newObj);
    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

    // check internal state

    sqlService.getConnection();

    Object objFromDb = sqlService.getObject(bucket1.bucketName, "key1");

    Assert.assertTrue(objFromDb.key.equals("key1"));
    sqlService.releaseConnection();
    Assert.assertTrue(objectStore.objectExists(objFromDb));

    Assert.assertTrue(IOUtils.toString(objectStore.getInputStream(objFromDb)).equals("data2"));

    // check external state

    Assert.assertTrue(repoService.getObjectVersions(objFromDb).size() == 2);
    Assert.assertTrue(repoService.listObjects(bucket1.bucketName).size() == 2);

    Assert.assertTrue(IOUtils.toString(repoService.getObjectInputStream(
            repoService.getObject(bucket1.bucketName, "key1", 0))).equals("data1")
    );

    Assert.assertTrue(IOUtils.toString(repoService.getObjectInputStream(
      repoService.getObject(bucket1.bucketName, "key1", 1))).equals("data2")
    );

    Assert.assertTrue(repoService.getObjectContentType(objFromDb).equals("new content type"));
    Assert.assertTrue(repoService.getObjectExportFileName(objFromDb).equals("new+download+name"));
  }

  @Test
  public void createObjectVersionNoBucket() throws Exception {

    repoService.createBucket("bucket0");

    try {
      repoService.createNewObject("key1", bucket1.bucketName, null, null, new Timestamp(new Date().getTime()), IOUtils.toInputStream("data1"));
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ClientError);
      Assert.assertTrue(e.getMessage().startsWith("Can not find bucket"));
    }

    // check state

    sqlService.getConnection();

    Object objFromDb = sqlService.getObject(bucket1.bucketName, "key1");

    Assert.assertTrue(objFromDb == null);
    sqlService.releaseConnection();
    Assert.assertFalse(objectStore.objectExists(new Object(null, null, "cbcc2ff6a0894e6e7f9a1a6a6a36b68fb36aa151", null, null, null, 0l, null, null, bucket1.bucketName, null, null)));

  }


  @Test
  public void updateObjectRollback() throws Exception {

    repoService.createBucket(bucket1.bucketName);

    Object newObj = repoService.createNewObject("key1", bucket1.bucketName, "first content type", "first download name", new Timestamp(new Date().getTime()), IOUtils.toInputStream("data1"));

    SqlService spySqlService = Mockito.spy(sqlService);

    BDDMockito.willReturn(0).given(spySqlService).insertObject(Mockito.any(Object.class));

    Field sqlServiceField = RepoService.class.getDeclaredField("sqlService");
    sqlServiceField.setAccessible(true);
    sqlServiceField.set(repoService, spySqlService);

    try {
      repoService.updateObject(bucket1.bucketName, "new content type", "new download name", new Timestamp(new Date().getTime()), IOUtils.toInputStream("data2"), newObj);

      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ServerError);
      Assert.assertTrue(e.getMessage().startsWith("Error saving content to database"));
    }

    // check state

    sqlService.getConnection();
    Object objFromDb = sqlService.getObject(bucket1.bucketName, "key1");
    Assert.assertTrue(objFromDb != null);
    sqlService.releaseConnection();
    Assert.assertTrue(objectStore.objectExists(new Object(null, null, "cbcc2ff6a0894e6e7f9a1a6a6a36b68fb36aa151", null, null, null, 0l, null, null, bucket1.bucketName, null, null)));  // since we do not delete object data
  }


  @Test
  public void updateObjectMetadata() throws Exception {

    repoService.createBucket(bucket1.bucketName);

    try {
      Object newObj = repoService.createNewObject("key1", bucket1.bucketName, "first content type", "first download name", new Timestamp(new Date().getTime()), IOUtils.toInputStream("data1"));
      repoService.updateObject(bucket1.bucketName, "new content type", "new download name", new Timestamp(new Date().getTime()), IOUtils.toInputStream(""), newObj);
    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

    // check internal state

    sqlService.getConnection();

    Object objFromDb = sqlService.getObject(bucket1.bucketName, "key1");

    Assert.assertTrue(objFromDb.key.equals("key1"));
    sqlService.releaseConnection();
    Assert.assertTrue(objectStore.objectExists(objFromDb));

    // check external state

    Assert.assertTrue(repoService.getObjectVersions(objFromDb).size() == 2);
    Assert.assertTrue(repoService.listObjects(bucket1.bucketName).size() == 2);

    Assert.assertTrue(IOUtils.toString(repoService.getObjectInputStream(
        repoService.getObject(bucket1.bucketName, "key1", 0))).equals("data1")
    );

    Assert.assertTrue(IOUtils.toString(repoService.getObjectInputStream(
        repoService.getObject(bucket1.bucketName, "key1", 1))).equals("data1")
    );

    Assert.assertTrue(repoService.getObjectContentType(objFromDb).equals("new content type"));
    Assert.assertTrue(repoService.getObjectExportFileName(objFromDb).equals("new+download+name"));

  }

  @Test
  public void deleteObject() throws Exception {

    repoService.createBucket(bucket1.bucketName);

    try {
      Object newObj = repoService.createNewObject("key1", bucket1.bucketName, null, null, new Timestamp(new Date().getTime()), IOUtils.toInputStream("data1"));
      repoService.updateObject(bucket1.bucketName, null, null, new Timestamp(new Date().getTime()), IOUtils.toInputStream("data2"), newObj);

      repoService.deleteObject(bucket1.bucketName, "key1", 1);

    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

    // check state

    sqlService.getConnection();

    Object objFromDb = sqlService.getObject(bucket1.bucketName, "key1");

    Assert.assertTrue(objFromDb.key.equals("key1"));
    sqlService.releaseConnection();
    Assert.assertTrue(objectStore.objectExists(objFromDb));

    Assert.assertTrue(IOUtils.toString(objectStore.getInputStream(objFromDb)).equals("data1"));


    Assert.assertTrue(repoService.getObjectVersions(objFromDb).size() == 1);
    Assert.assertTrue(repoService.listObjects(bucket1.bucketName).size() == 1);

  }

  @Test
  public void deleteObjectNotFound() throws Exception {

    repoService.createBucket(bucket1.bucketName);

    try {
      Object newObj = repoService.createNewObject("key1", bucket1.bucketName, null, null, new Timestamp(new Date().getTime()), IOUtils.toInputStream("data1"));
      repoService.updateObject(bucket1.bucketName, null, null, new Timestamp(new Date().getTime()), IOUtils.toInputStream("data2"), newObj);

      repoService.deleteObject(bucket1.bucketName, "key1", 5);

    } catch (RepoException e) {
      Assert.assertTrue(e.getMessage().startsWith("Object not found"));
    }

    // check state

    sqlService.getConnection();

    Object objFromDb = sqlService.getObject(bucket1.bucketName, "key1");

    Assert.assertTrue(objFromDb.key.equals("key1"));
    sqlService.releaseConnection();
    Assert.assertTrue(objectStore.objectExists(objFromDb));

    Assert.assertTrue(repoService.getObjectVersions(objFromDb).size() == 2);
    Assert.assertTrue(repoService.listObjects(bucket1.bucketName).size() == 2);
  }

  @Test
  public void getObjectReproxy() throws Exception {

    repoService.createBucket(bucket1.bucketName);

    Object newObj = repoService.createNewObject("key1", bucket1.bucketName, null, null, new Timestamp(new Date().getTime()), IOUtils.toInputStream("data1"));

    if (repoService.serverSupportsReproxy()) {

      Assert.assertTrue(repoService.getObjectReproxy(newObj).length > 0);

      // TODO: get the URLs and verify their downloaded content

    } else {
      Assert.assertTrue(repoService.getObjectReproxy(newObj).length == 0);
    }
  }

}
