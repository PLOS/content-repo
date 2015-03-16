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

 import com.google.common.base.Optional;
 import org.apache.commons.io.IOUtils;
 import org.junit.Assert;
 import org.junit.Before;
 import org.junit.Test;
 import org.mockito.BDDMockito;
 import org.mockito.Mockito;
 import org.plos.repo.models.Bucket;
 import org.plos.repo.models.Audit;
 import org.plos.repo.models.Operation;
 import org.plos.repo.models.RepoObject;
 import org.plos.repo.models.Status;
 import org.plos.repo.models.input.ElementFilter;
 import org.plos.repo.models.input.InputRepoObject;
 import org.plos.repo.service.RepoException;
 import org.plos.repo.service.RepoService;
 import org.plos.repo.service.SqlService;
 import org.plos.repo.service.BaseRepoService;
 import org.plos.repo.service.ObjectStore;

 import java.io.InputStream;
 import java.lang.reflect.Field;
 import java.sql.Timestamp;
 import java.util.Calendar;
 import java.util.Date;
 import java.util.List;

 import static org.plos.repo.service.BaseRepoService.AUDITING_ENABLED;

 public class RepoServiceSpringTest extends RepoBaseSpringTest {

  private static final String KEY = "key1";
  private static final Bucket bucket1 = new Bucket("bucket1");
  private static final Timestamp CREATION_DATE_TIME = new Timestamp(new Date().getTime());
  private static final String CREATION_DATE_TIME_STRING = CREATION_DATE_TIME.toString();
  private static final String CONTENT_TYPE = "text/plain";
  private static final String DOWNLOAD_NAME = "dlName";

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

    sqlService.getReadOnlyConnection();
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
    
    sqlService.getReadOnlyConnection();
    Assert.assertTrue(sqlService.getBucket(bucket1.getBucketName()) != null);
    if (AUDITING_ENABLED) {
      Assert.assertTrue(sqlService.listAudit(bucket1.getBucketName(), null, null, null, null).size() > 0);
    }
    sqlService.releaseConnection();
    Assert.assertTrue(objectStore.bucketExists(bucket1).get());
  }

  @Test
  public void createBucketWithBucketlessObjectStore() throws Exception {

    ObjectStore spyObjectStore = Mockito.spy(objectStore);

    BDDMockito.willReturn(Optional.absent()).given(spyObjectStore).bucketExists(Mockito.any(Bucket.class));

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

    BDDMockito.willReturn(Optional.of(false)).given(spyObjectStore).createBucket(Mockito.any(Bucket.class));

    Field objStoreField = RepoService.class.getDeclaredField("objectStore");
    objStoreField.setAccessible(true);
    objStoreField.set(repoService, spyObjectStore);

    try {
      List<Audit> audit = null;
      if (AUDITING_ENABLED) {
        audit = sqlService.listAudit(bucket1.getBucketName(), null, null, null, null);
      }
      repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);
      Assert.assertTrue(!AUDITING_ENABLED || sqlService.listAudit(bucket1.getBucketName(), null, null, null, null).size() == audit.size());
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ServerError);
      Assert.assertTrue(e.getMessage().startsWith("Unable to create bucket in object store"));
    }

    // check db state

    sqlService.getReadOnlyConnection();
    Assert.assertTrue(sqlService.getBucket(bucket1.getBucketName()) == null);
    sqlService.releaseConnection();
    Assert.assertFalse(objectStore.bucketExists(bucket1).get());
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

    sqlService.getReadOnlyConnection();
    if (AUDITING_ENABLED) {
      List<Audit> auditList = sqlService.listAudit(bucket1.getBucketName(), null, null, null, null);
      Assert.assertTrue(sqlService.getBucket(bucket1.getBucketName()) == null);
      Assert.assertTrue(sqlService.listAudit(bucket1.getBucketName(), null, null, null, null).size() == auditList.size());
    }
    sqlService.releaseConnection();
    Assert.assertFalse(objectStore.bucketExists(bucket1).get());

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

    sqlService.getReadOnlyConnection();
    Assert.assertTrue(sqlService.getBucket(bucket1.getBucketName()) == null);
    if (AUDITING_ENABLED) {
      List<Audit> auditList = sqlService.listAudit(bucket1.getBucketName(), null, null, Operation.CREATE_BUCKET, null);
      Assert.assertTrue(auditList.get(0).getOperation().equals(Operation.CREATE_BUCKET));
      auditList = sqlService.listAudit(bucket1.getBucketName(), null, null, Operation.DELETE_BUCKET, null);
      Assert.assertTrue(auditList.get(0).getOperation().equals(Operation.DELETE_BUCKET));
    }
    sqlService.releaseConnection();
    Assert.assertFalse(objectStore.bucketExists(bucket1).get());

  }

  @Test
  public void deleteBucketNotFoundInObjStore() throws Exception {

    ObjectStore spyObjectStore = Mockito.spy(objectStore);

    BDDMockito.willReturn(Optional.of(false)).given(spyObjectStore).bucketExists(Mockito.any(Bucket.class));

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
      repoService.createObject(RepoService.CreateMethod.NEW, createInputRepoObject());
    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

    // check state
    sqlService.getReadOnlyConnection();

    RepoObject objFromDb = sqlService.getObject(bucket1.getBucketName(), KEY);

    Assert.assertTrue(objFromDb.getKey().equals(KEY));
    if (AUDITING_ENABLED) {
      List<Audit> auditList = sqlService.listAudit(bucket1.getBucketName(), KEY, objFromDb.getVersionChecksum(), null, null);
      Audit audit = auditList.get(0);
      Assert.assertTrue(audit.getBucket().equals(bucket1.getBucketName()));
      Assert.assertTrue(audit.getKey().equals(KEY));
      Assert.assertTrue(audit.getOperation().equals(Operation.CREATE_OBJECT));
    }
    sqlService.releaseConnection();

    Assert.assertTrue(objectStore.objectExists(objFromDb));
    Assert.assertTrue(IOUtils.toString(objectStore.getInputStream(objFromDb)).equals("data1"));

    Assert.assertTrue(repoService.getObjectVersions(objFromDb.getBucketName(), objFromDb.getKey()).size() == 1);
    Assert.assertTrue(repoService.getObjectContentType(objFromDb).equals(CONTENT_TYPE));
    Assert.assertTrue(repoService.getObjectExportFileName(objFromDb).equals(DOWNLOAD_NAME));
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
      repoService.createObject(RepoService.CreateMethod.NEW, createInputRepoObject());
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ServerError);
      Assert.assertTrue(e.getMessage().startsWith("Error saving content to object store"));
    }

    // check state
    sqlService.getReadOnlyConnection();
    RepoObject objFromDb = sqlService.getObject(bucket1.getBucketName(), KEY);
    Assert.assertTrue(objFromDb == null);
    sqlService.releaseConnection();
    RepoObject repoObject = new RepoObject();
    repoObject.setChecksum("cbcc2ff6a0894e6e7f9a1a6a6a36b68fb36aa151");
    repoObject.setSize(0l);
    repoObject.setBucketName(bucket1.getBucketName());
    Assert.assertFalse(objectStore.objectExists(repoObject));
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
      repoService.createObject(RepoService.CreateMethod.NEW, createInputRepoObject());
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ServerError);
      Assert.assertTrue(e.getMessage().startsWith("Error saving content to database"));
    }

    // check state
    sqlService.getReadOnlyConnection();
    RepoObject objFromDb = sqlService.getObject(bucket1.getBucketName(), KEY);

      Assert.assertTrue(objFromDb == null);
    sqlService.releaseConnection();
    RepoObject repoObject = new RepoObject();
    repoObject.setChecksum("cbcc2ff6a0894e6e7f9a1a6a6a36b68fb36aa151");
    repoObject.setSize(0l);
    repoObject.setBucketName(bucket1.getBucketName());
    Assert.assertTrue(objectStore.objectExists(repoObject));  // since we do not delete object data
  }

  @Test
  public void createObjectVersion() throws Exception {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);

    try {
      InputRepoObject inputRepoObject = createInputRepoObject();
      repoService.createObject(RepoService.CreateMethod.NEW, inputRepoObject);

      Timestamp creationDateTimeObj2 = new Timestamp(new Date().getTime());
      inputRepoObject.setContentType("new content type");
      inputRepoObject.setDownloadName("new download name");
      inputRepoObject.setTimestamp(creationDateTimeObj2.toString());
      inputRepoObject.setUploadedInputStream(IOUtils.toInputStream("data2"));
      inputRepoObject.setCreationDateTime(creationDateTimeObj2.toString());
      repoService.createObject(RepoService.CreateMethod.VERSION, inputRepoObject);

    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

    // check internal state
    sqlService.getReadOnlyConnection();

    RepoObject objFromDb = sqlService.getObject(bucket1.getBucketName(), KEY);
    Assert.assertTrue(objFromDb.getKey().equals(KEY));
    if (AUDITING_ENABLED) {
      List<Audit> auditList = sqlService.listAudit(bucket1.getBucketName(), KEY, objFromDb.getVersionChecksum(), Operation.UPDATE_OBJECT, null);
      Assert.assertTrue(auditList.size() == 1);
      Assert.assertTrue(auditList.get(0).getOperation().equals(Operation.UPDATE_OBJECT));
    }
    sqlService.releaseConnection();

    Assert.assertTrue(objectStore.objectExists(objFromDb));
    Assert.assertTrue(IOUtils.toString(objectStore.getInputStream(objFromDb)).equals("data2"));

    // check external state
    Assert.assertTrue(repoService.getObjectVersions(objFromDb.getBucketName(), objFromDb.getKey()).size() == 2);
    Assert.assertTrue(repoService.listObjects(bucket1.getBucketName(), null, null, false, false, null).size() == 2);

    Assert.assertTrue(IOUtils.toString(repoService.getObjectInputStream(
            repoService.getObject(bucket1.getBucketName(), KEY, new ElementFilter(0, null, null)))).equals("data1")
    );

    Assert.assertTrue(IOUtils.toString(repoService.getObjectInputStream(
      repoService.getObject(bucket1.getBucketName(), KEY, new ElementFilter(1, null, null)))).equals("data2")
    );

    Assert.assertTrue(repoService.getObjectContentType(objFromDb).equals("new content type"));
    Assert.assertTrue(repoService.getObjectExportFileName(objFromDb).equals("new+download+name"));
  }

  @Test
  public void createObjectVersionNoBucket() throws Exception {

    repoService.createBucket("bucket0", CREATION_DATE_TIME_STRING);

    try {
      repoService.createObject(RepoService.CreateMethod.NEW, createInputRepoObject());
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.BucketNotFound);
      Assert.assertTrue(e.getMessage().equals(RepoException.Type.BucketNotFound.getMessage()));
    }

    // check state
    sqlService.getReadOnlyConnection();

    RepoObject objFromDb = sqlService.getObject(bucket1.getBucketName(), KEY);
    Assert.assertTrue(objFromDb == null);

    sqlService.releaseConnection();

    RepoObject repoObject = new RepoObject();
    repoObject.setChecksum("cbcc2ff6a0894e6e7f9a1a6a6a36b68fb36aa151");
    repoObject.setSize(0l);
    repoObject.setBucketName(bucket1.getBucketName());
    Assert.assertFalse(objectStore.objectExists(repoObject));

  }


  @Test
  public void updateObjectRollback() throws Exception {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);

    InputRepoObject inputRepoObject = createInputRepoObject();
    repoService.createObject(RepoService.CreateMethod.NEW, inputRepoObject);

    SqlService spySqlService = Mockito.spy(sqlService);

    BDDMockito.willReturn(0).given(spySqlService).insertObject(Mockito.any(RepoObject.class));

    Field sqlServiceField = BaseRepoService.class.getDeclaredField("sqlService");
    sqlServiceField.setAccessible(true);
    sqlServiceField.set(repoService, spySqlService);

    try {

      inputRepoObject.setContentType("new content type");
      inputRepoObject.setDownloadName("new download name");
      inputRepoObject.setUploadedInputStream(IOUtils.toInputStream("data2"));
      repoService.createObject(RepoService.CreateMethod.VERSION, inputRepoObject);

      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ServerError);
      Assert.assertTrue(e.getMessage().startsWith("Error saving content to database"));
    }

    // check state
    sqlService.getReadOnlyConnection();
    RepoObject objFromDb = sqlService.getObject(bucket1.getBucketName(), KEY);

    Assert.assertTrue(objFromDb != null);
    if (AUDITING_ENABLED) {
      List<Audit> audit = sqlService.listAudit(bucket1.getBucketName(), KEY, null, null, CREATION_DATE_TIME);
      Assert.assertTrue(!audit.get(audit.size() - 1).getOperation().equals(Operation.UPDATE_OBJECT));
    }
    sqlService.releaseConnection();

    RepoObject repoObject = new RepoObject();
    repoObject.setChecksum("cbcc2ff6a0894e6e7f9a1a6a6a36b68fb36aa151");
    repoObject.setSize(0l);
    repoObject.setBucketName(bucket1.getBucketName());
    Assert.assertTrue(objectStore.objectExists(repoObject));  // since we do not delete object data
  }


  @Test
  public void updateObjectMetadata() throws Exception {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);

    try {
      InputRepoObject inputRepoObject = createInputRepoObject();
      repoService.createObject(RepoService.CreateMethod.NEW, inputRepoObject);

      Timestamp creationDateObj2 = new Timestamp(new Date().getTime());
      inputRepoObject.setContentType("new content type");
      inputRepoObject.setDownloadName("new download name");
      inputRepoObject.setUploadedInputStream(IOUtils.toInputStream(""));
      inputRepoObject.setTimestamp(creationDateObj2.toString());
      inputRepoObject.setCreationDateTime(creationDateObj2.toString());
      repoService.createObject(RepoService.CreateMethod.VERSION, inputRepoObject);

    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

    // check internal state
    sqlService.getReadOnlyConnection();

    RepoObject objFromDb = sqlService.getObject(bucket1.getBucketName(), KEY);
    Assert.assertTrue(objFromDb.getKey().equals(KEY));
    if (AUDITING_ENABLED) {
      List<Audit> auditList = sqlService.listAudit(bucket1.getBucketName(), KEY, objFromDb.getVersionChecksum(), null, null);
      Assert.assertTrue(auditList.size() > 0);
      Assert.assertTrue(auditList.get(0).getOperation().equals(Operation.UPDATE_OBJECT));
    }
    sqlService.releaseConnection();

    Assert.assertTrue(objectStore.objectExists(objFromDb));

    // check external state

    Assert.assertTrue(repoService.getObjectVersions(objFromDb.getBucketName(), objFromDb.getKey()).size() == 2);
    Assert.assertTrue(repoService.listObjects(bucket1.getBucketName(), null, null, false,false, null).size() == 2);

    Assert.assertTrue(IOUtils.toString(repoService.getObjectInputStream(
        repoService.getObject(bucket1.getBucketName(), KEY, new ElementFilter(0, null, null)))).equals("data1")
    );

    Assert.assertTrue(IOUtils.toString(repoService.getObjectInputStream(
        repoService.getObject(bucket1.getBucketName(), KEY, new ElementFilter(1, null, null)))).equals("")
    );

    Assert.assertTrue(repoService.getObjectContentType(objFromDb).equals("new content type"));
    Assert.assertTrue(repoService.getObjectExportFileName(objFromDb).equals("new+download+name"));

  }

  @Test
  public void deleteObject() throws Exception {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);

    try {

      InputRepoObject inputRepoObject = createInputRepoObject();
        inputRepoObject.setContentType(null);
      inputRepoObject.setDownloadName(null);
        repoService.createObject(RepoService.CreateMethod.NEW, inputRepoObject);

      Timestamp creationDateObj2 = new Timestamp(new Date().getTime());
      inputRepoObject.setUploadedInputStream(IOUtils.toInputStream("data2"));
      inputRepoObject.setTimestamp(creationDateObj2.toString());
      inputRepoObject.setCreationDateTime(creationDateObj2.toString());
      repoService.createObject(RepoService.CreateMethod.VERSION, inputRepoObject);

      repoService.deleteObject(bucket1.getBucketName(), KEY, false, new ElementFilter(1, null, null));


    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

    // check state
    sqlService.getReadOnlyConnection();

    RepoObject objFromDb = sqlService.getObject(bucket1.getBucketName(), KEY);
    Assert.assertTrue(objFromDb.getKey().equals(KEY));
    if (AUDITING_ENABLED) {
      List<Audit> auditList = sqlService.listAudit(bucket1.getBucketName(), KEY, null, Operation.DELETE_OBJECT, null);
      Assert.assertTrue(auditList.size() > 0);
      Assert.assertTrue(auditList.get(0).getOperation().equals(Operation.DELETE_OBJECT));
    }
    sqlService.releaseConnection();

      Assert.assertTrue(objectStore.objectExists(objFromDb));
    Assert.assertTrue(IOUtils.toString(objectStore.getInputStream(objFromDb)).equals("data1"));

    Assert.assertTrue(repoService.getObjectVersions(objFromDb.getBucketName(), objFromDb.getKey()).size() == 1);

    Assert.assertTrue(repoService.listObjects(bucket1.getBucketName(), null, null, false, false, null).size() == 1);
    Assert.assertTrue(repoService.listObjects(bucket1.getBucketName(), null, null, true, false, null).size() == 2);

  }

  @Test
  public void deleteObjectNotFound() throws Exception {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME.toString());

    try {

      InputRepoObject inputRepoObject = createInputRepoObject();
        repoService.createObject(RepoService.CreateMethod.NEW, inputRepoObject);

      inputRepoObject.setUploadedInputStream(IOUtils.toInputStream("data2"));
      repoService.createObject(RepoService.CreateMethod.VERSION, inputRepoObject);

      repoService.deleteObject(bucket1.getBucketName(), KEY, false, new ElementFilter(5, null, null));

    } catch (RepoException e) {
        Assert.assertTrue(e.getMessage().startsWith("Object not found"));
    }

    // check state
    sqlService.getReadOnlyConnection();
    RepoObject objFromDb = sqlService.getObject(bucket1.getBucketName(), "key1");
    Assert.assertTrue(objFromDb.getKey().equals("key1"));
    sqlService.releaseConnection();

    Assert.assertTrue(objectStore.objectExists(objFromDb));
    Assert.assertTrue(repoService.getObjectVersions(objFromDb.getBucketName(), objFromDb.getKey()).size() == 2);
      Assert.assertTrue(repoService.listObjects(bucket1.getBucketName(), null, null, false, false, null).size() == 2);
  }

  @Test
  public void deletePurgedObject() throws Exception {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME.toString());
    RepoObject repoObject = repoService.createObject(RepoService.CreateMethod.NEW, createInputRepoObject());
    repoService.deleteObject(bucket1.getBucketName(), "key1", new ElementFilter(0, null, null), Status.PURGED);

    try {
      repoService.deleteObject(bucket1.getBucketName(), "key1", new ElementFilter(5, null, null), Status.DELETED);
    } catch (RepoException e) {
      Assert.assertTrue(e.getMessage().startsWith("Object not found"));
    }
      
    // check state
      sqlService.getReadOnlyConnection();
    if (AUDITING_ENABLED) {
      List<Audit> auditList = sqlService.listAudit(bucket1.getBucketName(), KEY, repoObject.getVersionChecksum(), Operation.PURGE_OBJECT, null);
      Assert.assertTrue(auditList.size() > 0);
      Assert.assertTrue(auditList.get(0).getOperation().equals(Operation.PURGE_OBJECT));
    }
    sqlService.releaseConnection();      
      
  }

  @Test
  public void purgePurgedObject() throws Exception {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME.toString());
    repoService.createObject(RepoService.CreateMethod.NEW, createInputRepoObject());
    repoService.deleteObject(bucket1.getBucketName(), "key1", new ElementFilter(0, null, null), Status.PURGED);

    try {
      repoService.deleteObject(bucket1.getBucketName(), "key1", new ElementFilter(5, null, null), Status.PURGED);
    } catch (RepoException e) {
      Assert.assertTrue(e.getMessage().startsWith("Object not found"));
    }
  }

  @Test
  public void purgeObject() throws Exception {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);

    RepoObject object2 = null;

    try {

      InputRepoObject inputRepoObject = createInputRepoObject();
      repoService.createObject(RepoService.CreateMethod.NEW, inputRepoObject);

      String creationDateObj2 = new Timestamp(new Date().getTime()).toString();
      inputRepoObject.setTimestamp(creationDateObj2);
      inputRepoObject.setCreate(creationDateObj2);
      inputRepoObject.setUploadedInputStream(IOUtils.toInputStream("data2"));
      object2 = repoService.createObject(RepoService.CreateMethod.VERSION, inputRepoObject);

      repoService.deleteObject(bucket1.getBucketName(), "key1", true, new ElementFilter(1, null, null));

    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

    // check state
    sqlService.getReadOnlyConnection();

    RepoObject objFromDb = sqlService.getObject(bucket1.getBucketName(), KEY);

    Assert.assertTrue(objFromDb.getKey().equals(KEY));
    sqlService.releaseConnection();

    Assert.assertTrue(objectStore.objectExists(objFromDb));
    Assert.assertTrue(IOUtils.toString(objectStore.getInputStream(objFromDb)).equals("data1"));

    // verify that the purge object does not exists the DB
    Assert.assertNull(objectStore.getInputStream(object2));

    sqlService.getReadOnlyConnection();

    // verify that the purge object does not exists the file system
    Assert.assertNull(sqlService.getObject(bucket1.getBucketName(), "key1", null, object2.getVersionChecksum(), null));
    if (AUDITING_ENABLED) {
      List<Audit> auditList = sqlService.listAudit(bucket1.getBucketName(), "key1",  object2.getVersionChecksum(), Operation.PURGE_OBJECT, null);
      Assert.assertTrue(auditList.size() > 0);
      Assert.assertTrue(auditList.get(0).getOperation().equals(Operation.PURGE_OBJECT));
    }
    sqlService.releaseConnection();

    Assert.assertTrue(repoService.getObjectVersions(objFromDb.getBucketName(), objFromDb.getKey()).size() == 1);
    Assert.assertTrue(repoService.listObjects(bucket1.getBucketName(), null, null, false, false, null).size() == 1);
    Assert.assertTrue(repoService.listObjects(bucket1.getBucketName(), null, null, true, false, null).size() == 1);
    Assert.assertTrue(repoService.listObjects(bucket1.getBucketName(), null, null, false, true, null).size() == 2);
    Assert.assertTrue(repoService.listObjects(bucket1.getBucketName(), null, null, true, true, null).size() == 2);
  }

  @Test
  public void purgeObjectSameContent() throws Exception {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);

    RepoObject object1 = null;
    RepoObject object2 = null;

    String dataContent = "data1";
    InputStream data = IOUtils.toInputStream(dataContent);

    try {

      InputRepoObject inputRepoObject = createInputRepoObject();
      object1 = repoService.createObject(RepoService.CreateMethod.NEW, inputRepoObject);

      String creationDateObj2 = new Timestamp(new Date().getTime()).toString();
      inputRepoObject.setTimestamp(creationDateObj2);
      inputRepoObject.setCreationDateTime(creationDateObj2);
      inputRepoObject.setTag("obj2");
      inputRepoObject.setUploadedInputStream(null);
      object2 = repoService.createObject(RepoService.CreateMethod.VERSION, inputRepoObject);

      //purge object1
      repoService.deleteObject(bucket1.getBucketName(), "key1", new ElementFilter(null, null, object1.getVersionChecksum()), Status.PURGED);

    } catch (RepoException e) {
      Assert.fail(e.getMessage());
    }

    // check state
    sqlService.getConnection();

    // verify that the only object in the DB for objKey="key1" is the last one created
    RepoObject obj2FromDb = sqlService.getObject(bucket1.getBucketName(), "key1");
    Assert.assertEquals("obj2", obj2FromDb.getTag());

    // verify that the first object created has been purge
    RepoObject obj1FromDb = sqlService.getObject(bucket1.getBucketName(), "key1", null, object1.getVersionChecksum(), null);
    Assert.assertNull(obj1FromDb);

      sqlService.releaseConnection();

    Assert.assertTrue(obj2FromDb.getKey().equals("key1"));
    Assert.assertTrue(objectStore.objectExists(obj2FromDb));
    Assert.assertTrue(IOUtils.toString(objectStore.getInputStream(obj2FromDb)).equals(dataContent));

    // verify that at service level we can't get the meta info for object1. object1 has been purged
    try{
      repoService.getObject(bucket1.getBucketName(), "key1", new ElementFilter(null, null, object1.getVersionChecksum()));
      Assert.fail("A repo exception was expected. ");
    } catch (RepoException e){
      Assert.assertEquals(e.getType(), RepoException.Type.ObjectNotFound);
    }

    // since the content of object1 has not been purged, because another object in the same bucket (object2), we can still retrieve the content
    // using the keys bucketName & checksum
    InputStream contentObj1 = repoService.getObjectInputStream(object1);
    InputStream contentObj2 = repoService.getObjectInputStream(object2);
      Assert.assertTrue(IOUtils.toString(contentObj1).equals(IOUtils.toString(contentObj2)));

    Assert.assertTrue(repoService.getObjectVersions(obj2FromDb.getBucketName(), obj2FromDb.getKey()).size() == 1);
    Assert.assertTrue(repoService.listObjects(bucket1.getBucketName(), null, null, false, false, null).size() == 1);
    Assert.assertTrue(repoService.listObjects(bucket1.getBucketName(), null, null, true, false, null).size() == 1);
    Assert.assertTrue(repoService.listObjects(bucket1.getBucketName(), null, null, false, true, null).size() == 2);
    Assert.assertTrue(repoService.listObjects(bucket1.getBucketName(), null, null, true, true, null).size() == 2);
  }

  @Test
  public void getObjectReproxy() throws Exception {

    repoService.createBucket(bucket1.getBucketName(), CREATION_DATE_TIME_STRING);

    RepoObject newObj = repoService.createObject(RepoService.CreateMethod.NEW, createInputRepoObject());

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

    // create object with creation date time
    InputRepoObject inputRepoObject = createInputRepoObject();
    inputRepoObject.setCreationDateTime(creationDateTime1.toString());
    inputRepoObject.setTimestamp(creationDateTime1.toString());
    repoService.createObject(RepoService.CreateMethod.NEW, inputRepoObject);

    cal.set(2014, 10, 20, 1, 1, 1);
    Timestamp creationDateTime2 = new Timestamp(cal.getTime().getTime());
    // create object with creation date time before object 1 creation date time
    inputRepoObject.setCreationDateTime(creationDateTime2.toString());
    inputRepoObject.setTimestamp(creationDateTime2.toString());
    inputRepoObject.setUploadedInputStream(IOUtils.toInputStream("data2"));
    repoService.createObject(RepoService.CreateMethod.VERSION, inputRepoObject);

    // get the latest object
    RepoObject repoObject = repoService.getObject(bucket1.getBucketName(), KEY, null);

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

    String key = KEY;
    // create object with creation date time
    InputRepoObject inputRepoObject = createInputRepoObject();
    inputRepoObject.setCreationDateTime(creationDateTime1.toString());
      inputRepoObject.setTimestamp(creationDateTime1.toString());
      inputRepoObject.setTag("DRAFT");
    repoService.createObject(RepoService.CreateMethod.NEW, inputRepoObject);

    cal.set(2014, 10, 20, 1, 1, 1);
    Timestamp creationDateTime2 = new Timestamp(cal.getTime().getTime());
    // create object with creation date time before object 1 creation date time
    inputRepoObject.setCreationDateTime(creationDateTime2.toString());
    inputRepoObject.setTimestamp(creationDateTime2.toString());
      inputRepoObject.setTag("FINAL");
    repoService.createObject(RepoService.CreateMethod.VERSION, inputRepoObject);

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

    // create object with creation date time
    InputRepoObject inputRepoObject = createInputRepoObject();
    inputRepoObject.setCreationDateTime(creationDateTime1.toString());
      inputRepoObject.setTimestamp(creationDateTime1.toString());
      inputRepoObject.setTag("FINAL");
      repoService.createObject(RepoService.CreateMethod.NEW, inputRepoObject);

      cal.set(2014, 10, 20, 1, 1, 1);
    Timestamp creationDateTime2 = new Timestamp(cal.getTime().getTime());
    // create object with creation date time before object 1 creation date time
      inputRepoObject.setCreationDateTime(creationDateTime2.toString());
      inputRepoObject.setTimestamp(creationDateTime2.toString());
    repoService.createObject(RepoService.CreateMethod.VERSION, inputRepoObject);

    // get the latest object
    RepoObject repoObject = repoService.getObject(bucket1.getBucketName(), KEY, new ElementFilter(null, "FINAL", null));

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

    // create object with creation date time
    InputRepoObject inputRepoObject = createInputRepoObject();
    inputRepoObject.setCreationDateTime(creationDateTime1.toString());
    inputRepoObject.setTimestamp(creationDateTime1.toString());
    inputRepoObject.setTag("DRAFT");
    repoService.createObject(RepoService.CreateMethod.NEW, inputRepoObject);

    cal.set(2014, 10, 20, 1, 1, 1);
    Timestamp creationDateTime2 = new Timestamp(cal.getTime().getTime());
    inputRepoObject.setCreationDateTime(creationDateTime2.toString());
    inputRepoObject.setUploadedInputStream(IOUtils.toInputStream("data2"));
    inputRepoObject.setTimestamp(creationDateTime2.toString());
    // create object with creation date time before object 1 creation date time
    RepoObject repoObject = repoService.createObject(RepoService.CreateMethod.VERSION, inputRepoObject);

    // get the latest object
    RepoObject resultRepoObject = repoService.getObject(bucket1.getBucketName(), KEY, new ElementFilter(null, null, repoObject.getVersionChecksum()));

    // object must match the one with the oldest creation time
    Assert.assertEquals(new Integer(1), resultRepoObject.getVersionNumber());
    Assert.assertEquals(creationDateTime2, resultRepoObject.getCreationDate());
    Assert.assertEquals(repoObject.getVersionChecksum(), resultRepoObject.getVersionChecksum());

  }

  private InputRepoObject createInputRepoObject(){
    InputRepoObject inputRepoObject = new InputRepoObject();
    inputRepoObject.setKey(KEY);
    inputRepoObject.setBucketName(bucket1.getBucketName());
    inputRepoObject.setContentType(CONTENT_TYPE);
    inputRepoObject.setDownloadName(DOWNLOAD_NAME);
    inputRepoObject.setTimestamp(CREATION_DATE_TIME_STRING);
    inputRepoObject.setUploadedInputStream(IOUtils.toInputStream("data1"));
    inputRepoObject.setCreationDateTime(CREATION_DATE_TIME_STRING);
    return inputRepoObject;
  }

}
