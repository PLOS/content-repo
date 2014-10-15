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
 import org.plos.repo.models.RepoObject;
 import org.plos.repo.models.input.ElementFilter;
 import org.plos.repo.service.*;

 import java.lang.reflect.Field;
 import java.sql.Timestamp;
 import java.util.Calendar;
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
    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);
    Assert.assertTrue(repoService.listBuckets().size() == 1);
  }

  @Test
  public void connectionAtomicityTest() throws Exception {

    sqlService.getConnection();

    Assert.assertTrue(sqlService.getBucket(bucket1.getBucketName()) == null);

    sqlService.insertBucket(bucket1, CREATION_DATE_TIME);

    Assert.assertTrue(sqlService.getBucket(bucket1.getBucketName()) != null);

    sqlService.transactionCommit();

    sqlService.getConnection();
    Assert.assertTrue(sqlService.getBucket(bucket1.getBucketName()) != null);
    sqlService.releaseConnection();

  }

  @Test
  public void createBucket() throws Exception {

    try {
      repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);
    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

    sqlService.getConnection();
    Assert.assertTrue(sqlService.getBucket(bucket1.getBucketName()) != null);
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
      Bucket bucketResponse = repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);
      Assert.assertEquals(bucket1.getBucketName(), bucketResponse.getBucketName());
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
      repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ServerError);
      Assert.assertTrue(e.getMessage().startsWith("Unable to create bucket in object store"));
    }

    // check db state

    sqlService.getConnection();
    Assert.assertTrue(sqlService.getBucket(bucket1.getBucketName()) == null);
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
      repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ServerError);
      Assert.assertTrue(e.getMessage().startsWith("Unable to create bucket in database"));
    }

    // check db state

    sqlService.getConnection();
    Assert.assertTrue(sqlService.getBucket(bucket1.getBucketName()) == null);
    sqlService.releaseConnection();
    Assert.assertFalse(objectStore.bucketExists(bucket1));

  }

  @Test
  public void deleteBucket() throws Exception {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);

    try {
      repoService.deleteBucket(bucket1.getBucketName());
    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

    Assert.assertTrue(repoService.listBuckets().size() == 0);

    sqlService.getConnection();
    Assert.assertTrue(sqlService.getBucket(bucket1.getBucketName()) == null);
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
      repoService.deleteBucket(bucket1.getBucketName());
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.BucketNotFound);
    }

  }

  @Test
  public void createNewObject() throws Exception {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);

    try {
      repoService.createObject(RepoService.CreateMethod.NEW, "key1", bucket1.getBucketName(), "text!", "dlName", CREATION_DATE_TIME, IOUtils.toInputStream("data1"), CREATION_DATE_TIME, null);
    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

    // check state

    sqlService.getConnection();

    RepoObject objFromDb = sqlService.getObject(bucket1.getBucketName(), "key1");

    Assert.assertTrue(objFromDb.getKey().equals("key1"));
    sqlService.releaseConnection();
    Assert.assertTrue(objectStore.objectExists(objFromDb));

    Assert.assertTrue(IOUtils.toString(objectStore.getInputStream(objFromDb)).equals("data1"));


    Assert.assertTrue(repoService.getObjectVersions(objFromDb.getBucketName(), objFromDb.getKey()).size() == 1);
    Assert.assertTrue(repoService.getObjectContentType(objFromDb).equals("text!"));
    Assert.assertTrue(repoService.getObjectExportFileName(objFromDb).equals("dlName"));
  }

  @Test
  public void createNewObjectUploadError() throws Exception {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);

    ObjectStore spyObjectStore = Mockito.spy(objectStore);

    BDDMockito.willReturn(false).given(spyObjectStore).saveUploadedObject(Mockito.any(Bucket.class), Mockito.any(ObjectStore.UploadInfo.class), Mockito.any(RepoObject.class));

    Field objStoreField = RepoService.class.getDeclaredField("objectStore");
    objStoreField.setAccessible(true);
    objStoreField.set(repoService, spyObjectStore);

    try {
      repoService.createObject(RepoService.CreateMethod.NEW, "key1", bucket1.getBucketName(), null, null, CREATION_DATE_TIME, IOUtils.toInputStream("data1"), CREATION_DATE_TIME, null);
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ServerError);
      Assert.assertTrue(e.getMessage().startsWith("Error saving content to object store"));
    }

    // check state

    sqlService.getConnection();
    RepoObject objFromDb = sqlService.getObject(bucket1.getBucketName(), "key1");
    Assert.assertTrue(objFromDb == null);
    sqlService.releaseConnection();
    Assert.assertFalse(objectStore.objectExists(new RepoObject(null, null, "cbcc2ff6a0894e6e7f9a1a6a6a36b68fb36aa151", null, null, null, 0l, null, null, bucket1.getBucketName(), null, null, null, null)));
  }

  @Test
  public void createNewObjectRollback() throws Exception {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);

    SqlService spySqlService = Mockito.spy(sqlService);

    BDDMockito.willReturn(0).given(spySqlService).insertObject(Mockito.any(RepoObject.class));

    Field sqlServiceField = BaseRepoService.class.getDeclaredField("sqlService");
    sqlServiceField.setAccessible(true);
    sqlServiceField.set(repoService, spySqlService);

    try {
      repoService.createObject(RepoService.CreateMethod.NEW, "key1", bucket1.getBucketName(), null, null, CREATION_DATE_TIME, IOUtils.toInputStream("data1"), CREATION_DATE_TIME, null);
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ServerError);
      Assert.assertTrue(e.getMessage().startsWith("Error saving content to database"));
    }

    // check state

    sqlService.getConnection();
    RepoObject objFromDb = sqlService.getObject(bucket1.getBucketName(), "key1");
    Assert.assertTrue(objFromDb == null);
    sqlService.releaseConnection();
    Assert.assertTrue(objectStore.objectExists(new RepoObject(null, null, "cbcc2ff6a0894e6e7f9a1a6a6a36b68fb36aa151", null, null, null, 0l, null, null, bucket1.getBucketName(), null, null, null, null)));  // since we do not delete object data
  }

  @Test
  public void createObjectVersion() throws Exception {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);

    try {

      repoService.createObject(RepoService.CreateMethod.NEW, "key1", bucket1.getBucketName(), "first content type", "first download name", CREATION_DATE_TIME, IOUtils.toInputStream("data1"), CREATION_DATE_TIME, null);
      Timestamp creationDateTimeObj2 = new Timestamp(new Date().getTime());
      repoService.createObject(RepoService.CreateMethod.VERSION, "key1", bucket1.getBucketName(), "new content type", "new download name", creationDateTimeObj2, IOUtils.toInputStream("data2"), creationDateTimeObj2, null);

    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

    // check internal state

    sqlService.getConnection();

    RepoObject objFromDb = sqlService.getObject(bucket1.getBucketName(), "key1");

    Assert.assertTrue(objFromDb.getKey().equals("key1"));
    sqlService.releaseConnection();
    Assert.assertTrue(objectStore.objectExists(objFromDb));

    Assert.assertTrue(IOUtils.toString(objectStore.getInputStream(objFromDb)).equals("data2"));

    // check external state

    Assert.assertTrue(repoService.getObjectVersions(objFromDb.getBucketName(), objFromDb.getKey()).size() == 2);
    Assert.assertTrue(repoService.listObjects(bucket1.getBucketName(), null, null, false, null).size() == 2);

    Assert.assertTrue(IOUtils.toString(repoService.getObjectInputStream(
            repoService.getObject(bucket1.getBucketName(), "key1", new ElementFilter(0, null, null)))).equals("data1")
    );

    Assert.assertTrue(IOUtils.toString(repoService.getObjectInputStream(
      repoService.getObject(bucket1.getBucketName(), "key1", new ElementFilter(1, null, null)))).equals("data2")
    );

    Assert.assertTrue(repoService.getObjectContentType(objFromDb).equals("new content type"));
    Assert.assertTrue(repoService.getObjectExportFileName(objFromDb).equals("new+download+name"));
  }

  @Test
  public void createObjectVersionNoBucket() throws Exception {

    repoService.createBucket("bucket0", CREATION_DATE_TIME_STRING);

    try {
      repoService.createObject(RepoService.CreateMethod.NEW, "key1", bucket1.getBucketName(), null, null, CREATION_DATE_TIME, IOUtils.toInputStream("data1"), CREATION_DATE_TIME, null);
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.BucketNotFound);
      Assert.assertTrue(e.getMessage().equals(RepoException.Type.BucketNotFound.getMessage()));
    }

    // check state

    sqlService.getConnection();

    RepoObject objFromDb = sqlService.getObject(bucket1.getBucketName(), "key1");

    Assert.assertTrue(objFromDb == null);
    sqlService.releaseConnection();
    Assert.assertFalse(objectStore.objectExists(new RepoObject(null, null, "cbcc2ff6a0894e6e7f9a1a6a6a36b68fb36aa151", null, null, null, 0l, null, null, bucket1.getBucketName(), null, null, null, null)));

  }


  @Test
  public void updateObjectRollback() throws Exception {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);

    repoService.createObject(RepoService.CreateMethod.NEW, "key1", bucket1.getBucketName(), "first content type", "first download name", CREATION_DATE_TIME, IOUtils.toInputStream("data1"), CREATION_DATE_TIME, null);

    SqlService spySqlService = Mockito.spy(sqlService);

    BDDMockito.willReturn(0).given(spySqlService).insertObject(Mockito.any(RepoObject.class));

    Field sqlServiceField = BaseRepoService.class.getDeclaredField("sqlService");
    sqlServiceField.setAccessible(true);
    sqlServiceField.set(repoService, spySqlService);

    try {

      repoService.createObject(RepoService.CreateMethod.VERSION, "key1", bucket1.getBucketName(), "new content type", "new download name", CREATION_DATE_TIME, IOUtils.toInputStream("data2"), CREATION_DATE_TIME, null);

      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ServerError);
      Assert.assertTrue(e.getMessage().startsWith("Error saving content to database"));
    }

    // check state

    sqlService.getConnection();
    RepoObject objFromDb = sqlService.getObject(bucket1.getBucketName(), "key1");
    Assert.assertTrue(objFromDb != null);
    sqlService.releaseConnection();
    Assert.assertTrue(objectStore.objectExists(new RepoObject(null, null, "cbcc2ff6a0894e6e7f9a1a6a6a36b68fb36aa151", null, null, null, 0l, null, null, bucket1.getBucketName(), null, null, null, null)));  // since we do not delete object data
  }


  @Test
  public void updateObjectMetadata() throws Exception {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);

    try {
      repoService.createObject(RepoService.CreateMethod.NEW, "key1", bucket1.getBucketName(), "first content type", "first download name", CREATION_DATE_TIME, IOUtils.toInputStream("data1"), CREATION_DATE_TIME, null);
      Timestamp creationDateObj2 = new Timestamp(new Date().getTime());
      repoService.createObject(RepoService.CreateMethod.VERSION, "key1", bucket1.getBucketName(), "new content type", "new download name", creationDateObj2, IOUtils.toInputStream(""), creationDateObj2, null);

    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

    // check internal state

    sqlService.getConnection();

    RepoObject objFromDb = sqlService.getObject(bucket1.getBucketName(), "key1");

    Assert.assertTrue(objFromDb.getKey().equals("key1"));
    sqlService.releaseConnection();
    Assert.assertTrue(objectStore.objectExists(objFromDb));

    // check external state

    Assert.assertTrue(repoService.getObjectVersions(objFromDb.getBucketName(), objFromDb.getKey()).size() == 2);
    Assert.assertTrue(repoService.listObjects(bucket1.getBucketName(), null, null, false, null).size() == 2);

    Assert.assertTrue(IOUtils.toString(repoService.getObjectInputStream(
        repoService.getObject(bucket1.getBucketName(), "key1", new ElementFilter(0, null, null)))).equals("data1")
    );

    Assert.assertTrue(IOUtils.toString(repoService.getObjectInputStream(
        repoService.getObject(bucket1.getBucketName(), "key1", new ElementFilter(1, null, null)))).equals("")
    );

    Assert.assertTrue(repoService.getObjectContentType(objFromDb).equals("new content type"));
    Assert.assertTrue(repoService.getObjectExportFileName(objFromDb).equals("new+download+name"));

  }

  @Test
  public void deleteObject() throws Exception {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);

    try {

      repoService.createObject(RepoService.CreateMethod.NEW, "key1", bucket1.getBucketName(), null, null, CREATION_DATE_TIME, IOUtils.toInputStream("data1"), CREATION_DATE_TIME, null);

      Timestamp creationDateObj2 = new Timestamp(new Date().getTime());
      repoService.createObject(RepoService.CreateMethod.VERSION, "key1", bucket1.getBucketName(), null, null, creationDateObj2, IOUtils.toInputStream("data2"), creationDateObj2, null);

      repoService.deleteObject(bucket1.getBucketName(), "key1", new ElementFilter(1, null, null));

    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

    // check state

    sqlService.getConnection();

    RepoObject objFromDb = sqlService.getObject(bucket1.getBucketName(), "key1");

    Assert.assertTrue(objFromDb.getKey().equals("key1"));
    sqlService.releaseConnection();
    Assert.assertTrue(objectStore.objectExists(objFromDb));

    Assert.assertTrue(IOUtils.toString(objectStore.getInputStream(objFromDb)).equals("data1"));


    Assert.assertTrue(repoService.getObjectVersions(objFromDb.getBucketName(), objFromDb.getKey()).size() == 1);
    Assert.assertTrue(repoService.listObjects(bucket1.getBucketName(), null, null, false, null).size() == 1);

    Assert.assertTrue(repoService.listObjects(bucket1.getBucketName(), null, null, true, null).size() == 2);
  }

  @Test
  public void deleteObjectNotFound() throws Exception {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME.toString());

    try {

      repoService.createObject(RepoService.CreateMethod.NEW, "key1", bucket1.getBucketName(), null, null, CREATION_DATE_TIME, IOUtils.toInputStream("data1"), CREATION_DATE_TIME, null);

      repoService.createObject(RepoService.CreateMethod.VERSION, "key1", bucket1.getBucketName(), null, null, CREATION_DATE_TIME, IOUtils.toInputStream("data2"), CREATION_DATE_TIME, null);

      repoService.deleteObject(bucket1.getBucketName(), "key1", new ElementFilter(5, null, null));

    } catch (RepoException e) {
      Assert.assertTrue(e.getMessage().startsWith("Object not found"));
    }

    // check state

    sqlService.getConnection();

    RepoObject objFromDb = sqlService.getObject(bucket1.getBucketName(), "key1");

    Assert.assertTrue(objFromDb.getKey().equals("key1"));
    sqlService.releaseConnection();
    Assert.assertTrue(objectStore.objectExists(objFromDb));

    Assert.assertTrue(repoService.getObjectVersions(objFromDb.getBucketName(), objFromDb.getKey()).size() == 2);
    Assert.assertTrue(repoService.listObjects(bucket1.getBucketName(), null, null, false, null).size() == 2);
  }

  @Test
  public void getObjectReproxy() throws Exception {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);

    RepoObject newObj = repoService.createObject(RepoService.CreateMethod.NEW, "key1", bucket1.getBucketName(), null, null, CREATION_DATE_TIME, IOUtils.toInputStream("data1"), CREATION_DATE_TIME, null);

    if (repoService.serverSupportsReproxy()) {

      Assert.assertTrue(repoService.getObjectReproxy(newObj).length > 0);

      // TODO: get the URLs and verify their downloaded content

    } else {
      Assert.assertTrue(repoService.getObjectReproxy(newObj).length == 0);
    }
  }

  @Test
  public void getLatestObjectTest() throws RepoException {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);

    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(0);
    cal.set(2014, 10, 30, 1, 1, 1);
    Timestamp creationDateTime1 = new Timestamp(cal.getTime().getTime());

    String key = "key1";
    // create object with creation date time
    repoService.createObject(RepoService.CreateMethod.NEW, key, bucket1.getBucketName(), null, null, creationDateTime1, IOUtils.toInputStream("data1"), creationDateTime1, null);

    cal.set(2014, 10, 20, 1, 1, 1);
    Timestamp creationDateTime2 = new Timestamp(cal.getTime().getTime());
    // create object with creation date time before object 1 creation date time
    repoService.createObject(RepoService.CreateMethod.VERSION, key, bucket1.getBucketName(), null, null, creationDateTime2, IOUtils.toInputStream("data2"), creationDateTime2, null);

    // get the latest object
    RepoObject repoObject = repoService.getObject(bucket1.getBucketName(), key, null);

    // object must match the one with the oldest creation time
    Assert.assertEquals(new Integer(0), repoObject.getVersionNumber());
    Assert.assertEquals(creationDateTime1, repoObject.getCreationDate());

  }

  @Test
  public void getObjectUsingTagTest() throws RepoException {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);

    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(0);
    cal.set(2014, 10, 30, 1, 1, 1);
    Timestamp creationDateTime1 = new Timestamp(cal.getTime().getTime());

    String key = "key1";
    // create object with creation date time
    repoService.createObject(RepoService.CreateMethod.NEW, key, bucket1.getBucketName(), null, null, creationDateTime1, IOUtils.toInputStream("data1"), creationDateTime1, "DRAFT");

    cal.set(2014, 10, 20, 1, 1, 1);
    Timestamp creationDateTime2 = new Timestamp(cal.getTime().getTime());
    // create object with creation date time before object 1 creation date time
    repoService.createObject(RepoService.CreateMethod.VERSION, key, bucket1.getBucketName(), null, null, creationDateTime2, IOUtils.toInputStream("data2"), creationDateTime2, "FINAL");

    // get the latest object
    RepoObject repoObject = repoService.getObject(bucket1.getBucketName(), key, new ElementFilter(null, "DRAFT", null));

    // object must match the one with the oldest creation time
    Assert.assertEquals(new Integer(0), repoObject.getVersionNumber());
    Assert.assertEquals(creationDateTime1, repoObject.getCreationDate());

  }

  @Test
  public void getLatestObjectUsingTagTest() throws RepoException {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);

    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(0);
    cal.set(2014, 10, 30, 1, 1, 1);
    Timestamp creationDateTime1 = new Timestamp(cal.getTime().getTime());

    String key = "key1";
    // create object with creation date time
    repoService.createObject(RepoService.CreateMethod.NEW, key, bucket1.getBucketName(), null, null, creationDateTime1, IOUtils.toInputStream("data1"), creationDateTime1, "FINAL");

    cal.set(2014, 10, 20, 1, 1, 1);
    Timestamp creationDateTime2 = new Timestamp(cal.getTime().getTime());
    // create object with creation date time before object 1 creation date time
    repoService.createObject(RepoService.CreateMethod.VERSION, key, bucket1.getBucketName(), null, null, creationDateTime2, IOUtils.toInputStream("data2"), creationDateTime2, "FINAL");

    // get the latest object
    RepoObject repoObject = repoService.getObject(bucket1.getBucketName(), key, new ElementFilter(null, "FINAL", null));

    // object must match the one with the oldest creation time
    Assert.assertEquals(new Integer(0), repoObject.getVersionNumber());
    Assert.assertEquals(creationDateTime1, repoObject.getCreationDate());

  }

  @Test
  public void getObjectUsingVersionChecksum() throws RepoException {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);

    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(0);
    cal.set(2014, 10, 30, 1, 1, 1);
    Timestamp creationDateTime1 = new Timestamp(cal.getTime().getTime());

    String key = "key1";
    // create object with creation date time
    repoService.createObject(RepoService.CreateMethod.NEW, key, bucket1.getBucketName(), null, null, creationDateTime1, IOUtils.toInputStream("data1"), creationDateTime1, "DRAFT");

    cal.set(2014, 10, 20, 1, 1, 1);
    Timestamp creationDateTime2 = new Timestamp(cal.getTime().getTime());
    // create object with creation date time before object 1 creation date time
    RepoObject repoObject = repoService.createObject(RepoService.CreateMethod.VERSION, key, bucket1.getBucketName(), null, null, creationDateTime2, IOUtils.toInputStream("data2"), creationDateTime2, "FINAL");

    // get the latest object
    RepoObject resultRepoObject = repoService.getObject(bucket1.getBucketName(), key, new ElementFilter(null, "FINAL", repoObject.getVersionChecksum()));

    // object must match the one with the oldest creation time
    Assert.assertEquals(new Integer(1), resultRepoObject.getVersionNumber());
    Assert.assertEquals(creationDateTime2, resultRepoObject.getCreationDate());
    Assert.assertEquals(repoObject.getVersionChecksum(), resultRepoObject.getVersionChecksum());

  }

}
