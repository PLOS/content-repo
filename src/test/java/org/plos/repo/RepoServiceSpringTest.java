 /* Copyright (c) 2006-2014 by Public Library of Science
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

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.ElementFilter;
import org.plos.repo.models.Object;
import org.plos.repo.service.*;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.Date;

public class RepoServiceSpringTest extends RepoBaseSpringTest {

  private final Bucket bucket1 = new Bucket("bucket1");

  private final Timestamp CREATION_DATE_TIME = new Timestamp(new Date().getTime());
  private final String CREATION_DATE_TIME_STRING = CREATION_DATE_TIME.toString();

  @Before
  public void beforeMethods() throws Exception {

    clearData(objectStore, sqlService);

    // reset the internal beans since their mocks/spies retain between tests

    Field objStoreField = RepoService.class.getDeclaredField("objectStore");
    objStoreField.setAccessible(true);
    objStoreField.set(repoService, objectStore);

    Field sqlServiceField = BaseRepoService.class.getDeclaredField("sqlService");
    sqlServiceField.setAccessible(true);
    sqlServiceField.set(repoService, sqlService);

  }

  @Test
  public void repoServiceNotNull() {
    Assert.assertNotNull(repoService);
  }

  @Test
  public void listBuckets() throws Exception {
    Assert.assertTrue(repoService.listBuckets().size() == 0);
    repoService.createBucket(bucket1.bucketName, CREATION_DATE_TIME_STRING);
    Assert.assertTrue(repoService.listBuckets().size() == 1);
  }

  @Test
  public void connectionAtomicityTest() throws Exception {

    sqlService.getConnection();

    Assert.assertTrue(sqlService.getBucket(bucket1.bucketName) == null);

    sqlService.insertBucket(bucket1, CREATION_DATE_TIME);

    Assert.assertTrue(sqlService.getBucket(bucket1.bucketName) != null);

    sqlService.transactionCommit();

    sqlService.getConnection();
    Assert.assertTrue(sqlService.getBucket(bucket1.bucketName) != null);
    sqlService.releaseConnection();

  }

  @Test
  public void createBucket() throws Exception {

    try {
      repoService.createBucket(bucket1.bucketName, CREATION_DATE_TIME_STRING);
    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

    sqlService.getConnection();
    Assert.assertTrue(sqlService.getBucket(bucket1.bucketName) != null);
    sqlService.releaseConnection();
    Assert.assertTrue(objectStore.bucketExists(bucket1));
  }

  @Test
  public void createBucketWithBucketlessObjectStore() throws Exception {

    ObjectStore spyObjectStore = Mockito.spy(objectStore);

    BDDMockito.willReturn(null).given(spyObjectStore).bucketExists(Mockito.any(Bucket.class));

    Field objStoreField = RepoService.class.getDeclaredField("objectStore");
    objStoreField.setAccessible(true);
    objStoreField.set(repoService, spyObjectStore);

    try {
      Bucket bucketResponse = repoService.createBucket(bucket1.bucketName, CREATION_DATE_TIME_STRING);
      Assert.assertEquals(bucket1.bucketName, bucketResponse.bucketName);
    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

  }

  @Test
  public void createBucketErrorInObjStore() throws Exception {

    ObjectStore spyObjectStore = Mockito.spy(objectStore);

    BDDMockito.willReturn(false).given(spyObjectStore).createBucket(Mockito.any(Bucket.class));

    Field objStoreField = RepoService.class.getDeclaredField("objectStore");
    objStoreField.setAccessible(true);
    objStoreField.set(repoService, spyObjectStore);

    try {
      repoService.createBucket(bucket1.bucketName, CREATION_DATE_TIME_STRING);
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ServerError);
      Assert.assertTrue(e.getMessage().startsWith("Unable to create bucket in object store"));
    }

    // check db state

    sqlService.getConnection();
    Assert.assertTrue(sqlService.getBucket(bucket1.bucketName) == null);
    sqlService.releaseConnection();
    Assert.assertFalse(objectStore.bucketExists(bucket1));
  }

  @Test
  public void createBucketErrorInDb() throws Exception {

    SqlService spySqlService = Mockito.spy(sqlService);

    BDDMockito.willReturn(false).given(spySqlService).insertBucket(Mockito.any(Bucket.class), Mockito.any(Timestamp.class));

    Field sqlServiceField = BaseRepoService.class.getDeclaredField("sqlService");
    sqlServiceField.setAccessible(true);
    sqlServiceField.set(repoService, spySqlService);

    try {
      repoService.createBucket(bucket1.bucketName, CREATION_DATE_TIME_STRING);
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ServerError);
      Assert.assertTrue(e.getMessage().startsWith("Unable to create bucket in database"));
    }

    // check db state

    sqlService.getConnection();
    Assert.assertTrue(sqlService.getBucket(bucket1.bucketName) == null);
    sqlService.releaseConnection();
    Assert.assertFalse(objectStore.bucketExists(bucket1));

  }

  @Test
  public void deleteBucket() throws Exception {

    repoService.createBucket(bucket1.bucketName, CREATION_DATE_TIME_STRING);

    try {
      repoService.deleteBucket(bucket1.bucketName);
    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

    Assert.assertTrue(repoService.listBuckets().size() == 0);

    sqlService.getConnection();
    Assert.assertTrue(sqlService.getBucket(bucket1.bucketName) == null);
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
      Assert.assertTrue(e.getType() == RepoException.Type.BucketNotFound);
    }

  }

  @Test
  public void createNewObject() throws Exception {

    repoService.createBucket(bucket1.bucketName, CREATION_DATE_TIME_STRING);

    try {
      repoService.createObject(RepoService.CreateMethod.NEW, "key1", bucket1.bucketName, "text!", "dlName", CREATION_DATE_TIME, IOUtils.toInputStream("data1"), CREATION_DATE_TIME);
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

    repoService.createBucket(bucket1.bucketName, CREATION_DATE_TIME_STRING);

    ObjectStore spyObjectStore = Mockito.spy(objectStore);

    BDDMockito.willReturn(false).given(spyObjectStore).saveUploadedObject(Mockito.any(Bucket.class), Mockito.any(ObjectStore.UploadInfo.class), Mockito.any(Object.class));

    Field objStoreField = RepoService.class.getDeclaredField("objectStore");
    objStoreField.setAccessible(true);
    objStoreField.set(repoService, spyObjectStore);

    try {
      repoService.createObject(RepoService.CreateMethod.NEW, "key1", bucket1.bucketName, null, null, CREATION_DATE_TIME, IOUtils.toInputStream("data1"), CREATION_DATE_TIME);
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
    Assert.assertFalse(objectStore.objectExists(new Object(null, null, "cbcc2ff6a0894e6e7f9a1a6a6a36b68fb36aa151", null, null, null, 0l, null, null, bucket1.bucketName, null, null, null, null)));
  }

  @Test
  public void createNewObjectRollback() throws Exception {

    repoService.createBucket(bucket1.bucketName, CREATION_DATE_TIME_STRING);

    SqlService spySqlService = Mockito.spy(sqlService);

    BDDMockito.willReturn(0).given(spySqlService).insertObject(Mockito.any(Object.class));

    Field sqlServiceField = BaseRepoService.class.getDeclaredField("sqlService");
    sqlServiceField.setAccessible(true);
    sqlServiceField.set(repoService, spySqlService);

    try {
      repoService.createObject(RepoService.CreateMethod.NEW, "key1", bucket1.bucketName, null, null, CREATION_DATE_TIME, IOUtils.toInputStream("data1"), CREATION_DATE_TIME);
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
    Assert.assertTrue(objectStore.objectExists(new Object(null, null, "cbcc2ff6a0894e6e7f9a1a6a6a36b68fb36aa151", null, null, null, 0l, null, null, bucket1.bucketName, null, null, null, null)));  // since we do not delete object data
  }

  @Test
  public void createObjectVersion() throws Exception {

    repoService.createBucket(bucket1.bucketName, CREATION_DATE_TIME_STRING);

    try {

      repoService.createObject(RepoService.CreateMethod.NEW, "key1", bucket1.bucketName, "first content type", "first download name", CREATION_DATE_TIME, IOUtils.toInputStream("data1"), CREATION_DATE_TIME);
      Timestamp creationDateTimeObj2 = new Timestamp(new Date().getTime());
      repoService.createObject(RepoService.CreateMethod.VERSION, "key1", bucket1.bucketName, "new content type", "new download name", creationDateTimeObj2, IOUtils.toInputStream("data2"), creationDateTimeObj2);

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
    Assert.assertTrue(repoService.listObjects(bucket1.bucketName, null, null, false, null).size() == 2);

    Assert.assertTrue(IOUtils.toString(repoService.getObjectInputStream(
            repoService.getObject(bucket1.bucketName, "key1", new ElementFilter(0, null, null)))).equals("data1")
    );

    Assert.assertTrue(IOUtils.toString(repoService.getObjectInputStream(
      repoService.getObject(bucket1.bucketName, "key1", new ElementFilter(1, null, null)))).equals("data2")
    );

    Assert.assertTrue(repoService.getObjectContentType(objFromDb).equals("new content type"));
    Assert.assertTrue(repoService.getObjectExportFileName(objFromDb).equals("new+download+name"));
  }

  @Test
  public void createObjectVersionNoBucket() throws Exception {

    repoService.createBucket("bucket0", CREATION_DATE_TIME_STRING);

    try {
      repoService.createObject(RepoService.CreateMethod.NEW, "key1", bucket1.bucketName, null, null, CREATION_DATE_TIME, IOUtils.toInputStream("data1"), CREATION_DATE_TIME);
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.BucketNotFound);
      Assert.assertTrue(e.getMessage().equals(RepoException.Type.BucketNotFound.getMessage()));
    }

    // check state

    sqlService.getConnection();

    Object objFromDb = sqlService.getObject(bucket1.bucketName, "key1");

    Assert.assertTrue(objFromDb == null);
    sqlService.releaseConnection();
    Assert.assertFalse(objectStore.objectExists(new Object(null, null, "cbcc2ff6a0894e6e7f9a1a6a6a36b68fb36aa151", null, null, null, 0l, null, null, bucket1.bucketName, null, null, null, null)));

  }


  @Test
  public void updateObjectRollback() throws Exception {

    repoService.createBucket(bucket1.bucketName, CREATION_DATE_TIME_STRING);

    repoService.createObject(RepoService.CreateMethod.NEW, "key1", bucket1.bucketName, "first content type", "first download name", CREATION_DATE_TIME, IOUtils.toInputStream("data1"), CREATION_DATE_TIME);

    SqlService spySqlService = Mockito.spy(sqlService);

    BDDMockito.willReturn(0).given(spySqlService).insertObject(Mockito.any(Object.class));

    Field sqlServiceField = BaseRepoService.class.getDeclaredField("sqlService");
    sqlServiceField.setAccessible(true);
    sqlServiceField.set(repoService, spySqlService);

    try {

      repoService.createObject(RepoService.CreateMethod.VERSION, "key1", bucket1.bucketName, "new content type", "new download name", CREATION_DATE_TIME, IOUtils.toInputStream("data2"), CREATION_DATE_TIME);

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
    Assert.assertTrue(objectStore.objectExists(new Object(null, null, "cbcc2ff6a0894e6e7f9a1a6a6a36b68fb36aa151", null, null, null, 0l, null, null, bucket1.bucketName, null, null, null, null)));  // since we do not delete object data
  }


  @Test
  public void updateObjectMetadata() throws Exception {

    repoService.createBucket(bucket1.bucketName, CREATION_DATE_TIME_STRING);

    try {
      repoService.createObject(RepoService.CreateMethod.NEW, "key1", bucket1.bucketName, "first content type", "first download name", CREATION_DATE_TIME, IOUtils.toInputStream("data1"), CREATION_DATE_TIME);
      Timestamp creationDateObj2 = new Timestamp(new Date().getTime());
      repoService.createObject(RepoService.CreateMethod.VERSION, "key1", bucket1.bucketName, "new content type", "new download name", creationDateObj2, IOUtils.toInputStream(""), creationDateObj2);

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
    Assert.assertTrue(repoService.listObjects(bucket1.bucketName, null, null, false, null).size() == 2);

    Assert.assertTrue(IOUtils.toString(repoService.getObjectInputStream(
        repoService.getObject(bucket1.bucketName, "key1", new ElementFilter(0, null, null)))).equals("data1")
    );

    Assert.assertTrue(IOUtils.toString(repoService.getObjectInputStream(
        repoService.getObject(bucket1.bucketName, "key1", new ElementFilter(1, null, null)))).equals("")
    );

    Assert.assertTrue(repoService.getObjectContentType(objFromDb).equals("new content type"));
    Assert.assertTrue(repoService.getObjectExportFileName(objFromDb).equals("new+download+name"));

  }

  @Test
  public void deleteObject() throws Exception {

    repoService.createBucket(bucket1.bucketName, CREATION_DATE_TIME_STRING);

    try {

      repoService.createObject(RepoService.CreateMethod.NEW, "key1", bucket1.bucketName, null, null, CREATION_DATE_TIME, IOUtils.toInputStream("data1"), CREATION_DATE_TIME);

      Timestamp creationDateObj2 = new Timestamp(new Date().getTime());
      repoService.createObject(RepoService.CreateMethod.VERSION, "key1", bucket1.bucketName, null, null, creationDateObj2, IOUtils.toInputStream("data2"), creationDateObj2);

      repoService.deleteObject(bucket1.bucketName, "key1", new ElementFilter(1, null, null));

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
    Assert.assertTrue(repoService.listObjects(bucket1.bucketName, null, null, false, null).size() == 1);

    Assert.assertTrue(repoService.listObjects(bucket1.bucketName, null, null, true, null).size() == 2);
  }

  @Test
  public void deleteObjectNotFound() throws Exception {

    repoService.createBucket(bucket1.bucketName, CREATION_DATE_TIME.toString());

    try {

      repoService.createObject(RepoService.CreateMethod.NEW, "key1", bucket1.bucketName, null, null, CREATION_DATE_TIME, IOUtils.toInputStream("data1"), CREATION_DATE_TIME);

      repoService.createObject(RepoService.CreateMethod.VERSION, "key1", bucket1.bucketName, null, null, CREATION_DATE_TIME, IOUtils.toInputStream("data2"), CREATION_DATE_TIME);

      repoService.deleteObject(bucket1.bucketName, "key1", new ElementFilter(5, null, null));

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
    Assert.assertTrue(repoService.listObjects(bucket1.bucketName, null, null, false, null).size() == 2);
  }

  @Test
  public void getObjectReproxy() throws Exception {

    repoService.createBucket(bucket1.bucketName, CREATION_DATE_TIME_STRING);

    Object newObj = repoService.createObject(RepoService.CreateMethod.NEW, "key1", bucket1.bucketName, null, null, CREATION_DATE_TIME, IOUtils.toInputStream("data1"), CREATION_DATE_TIME);

    if (repoService.serverSupportsReproxy()) {

      Assert.assertTrue(repoService.getObjectReproxy(newObj).length > 0);

      // TODO: get the URLs and verify their downloaded content

    } else {
      Assert.assertTrue(repoService.getObjectReproxy(newObj).length == 0);
    }
  }

}
