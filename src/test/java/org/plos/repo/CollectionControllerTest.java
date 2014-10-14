/*
 * Copyright (c) 2006-2014 by Public Library of Science
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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.junit.Before;
import org.junit.Test;
import org.plos.repo.models.input.InputCollection;
import org.plos.repo.models.input.InputObject;
import org.plos.repo.service.RepoException;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;

public class CollectionControllerTest extends RepoBaseJerseyTest {

  private final String bucketName = "plos-objstoreunittest-bucket1";
  private final String objectName1 = "object1";
  private final String contentType1 = "text/plain";

  private final String objectName2 = "object2";
  private final String contentType2 = "image/jpg";

  private final Timestamp CREATION_DATE_TIME = new Timestamp(new Date().getTime());
  private final String CREATION_DATE_TIME_STRING = CREATION_DATE_TIME.toString();

  private final String testData1 = "test data one goes\nhere.";


  @Before
  public void setup() throws Exception {
    RepoBaseSpringTest.clearData(objectStore, sqlService);
  }

  @Test
  public void createWithEmptyCreationType() {

    InputCollection inputCollection = new InputCollection();
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertRepoError(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity),
        Response.Status.BAD_REQUEST, RepoException.Type.NoCreationMethodEntered
    );
  }

  @Test
  public void createWithInvalidCreationType() {

    InputCollection inputCollection = new InputCollection();
    inputCollection.setCreate("invalidCreationMethod");
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertRepoError(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity),
        Response.Status.BAD_REQUEST, RepoException.Type.InvalidCreationMethod
    );
  }

  @Test
  public void createWithNoKey() {

    InputCollection inputCollection = new InputCollection();
    inputCollection.setCreate("new");
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertRepoError(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity),
        Response.Status.BAD_REQUEST, RepoException.Type.NoCollectionKeyEntered
    );
  }

  @Test
  public void createWithEmptyBucketName() {

    InputCollection inputCollection = new InputCollection();
    inputCollection.setCreate("new");
    inputCollection.setKey("emptyBucketName");
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertRepoError(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity),
        Response.Status.BAD_REQUEST, RepoException.Type.NoBucketEntered
    );
  }

  @Test
  public void createWithInvalidTimestamp() {

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("invalidTimeStamp");
    inputCollection.setCreate("new");
    inputCollection.setTimestamp("abc");
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertRepoError(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity),
        Response.Status.BAD_REQUEST, RepoException.Type.CouldNotParseTimestamp
    );
  }

  @Test
  public void createWithNoObjects() {

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("emptyObjects");
    inputCollection.setCreate("new");
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertRepoError(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity),
        Response.Status.BAD_REQUEST, RepoException.Type.CantCreateCollectionWithNoObjects
    );
  }

  @Test
  public void createWithInvalidBucket() {

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName("invalidBucket");
    inputCollection.setKey("invalidBucket");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{new InputObject("obj1","213i3b21312")}));
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertRepoError(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity),
        Response.Status.NOT_FOUND, RepoException.Type.BucketNotFound
    );
  }

  @Test
  public void createWithExistingKey(){

    generateBuckets(bucketName);
    String versionChecksumObj1 = createObject(bucketName, objectName1, contentType1);

    InputObject object1 = new InputObject(objectName1,versionChecksumObj1);

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("existingKey");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object1}));
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertEquals(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    assertRepoError(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity),
        Response.Status.BAD_REQUEST,
        RepoException.Type.CantCreateNewCollectionWithUsedKey
    );

  }

  @Test
  public void versionNonExistingCollection(){

    generateBuckets(bucketName);
    String versionChecksumObj1 = createObject(bucketName, objectName1, contentType1);
    String versionChecksumObj2 = createObject(bucketName, objectName2, contentType2);

    InputObject object1 = new InputObject(objectName1,versionChecksumObj1);
    InputObject object2 = new InputObject(objectName2,versionChecksumObj2);

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("existingKey");
    inputCollection.setCreate("version");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object1, object2}));
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertRepoError(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity),
        Response.Status.BAD_REQUEST,
        RepoException.Type.CantCreateCollectionVersionWithNoOrig
    );

  }

  @Test
  public void autoCreateCollection(){

    generateBuckets(bucketName);
    String versionChecksumObj1 = createObject(bucketName, objectName1, contentType1);
    String versionChecksumObj2 = createObject(bucketName, objectName2, contentType1);

    InputObject object1 = new InputObject(objectName1,versionChecksumObj1);
    InputObject object2 = new InputObject(objectName2,versionChecksumObj2);

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("existingKey");
    inputCollection.setCreate("auto");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object1, object2}));
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertEquals(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

  }

  @Test
  public void autoVersionExistingKeyCollection(){

    generateBuckets(bucketName);
    String versionChecksumObj1 = createObject(bucketName, objectName1, contentType1);
    String versionChecksumObj2 = createObject(bucketName, objectName2, contentType2);

    InputObject object1 = new InputObject(objectName1,versionChecksumObj1);
    InputObject object2 = new InputObject(objectName2,versionChecksumObj2);

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("existingKey");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object1, object2}));
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertEquals(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    assertRepoError(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity),
        Response.Status.BAD_REQUEST,
        RepoException.Type.CantCreateNewCollectionWithUsedKey
    );

  }

  @Test
  public void createColletionNonexistingObject(){

    generateBuckets(bucketName);

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("nonexistingKey");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{new InputObject("nonexistingKey", "789789")}));
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertRepoError(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity),
        Response.Status.NOT_FOUND,
        RepoException.Type.ObjectCollectionNotFound
    );

    Response response = target("/collections/" + bucketName)
        .queryParam("version", "0")
        .queryParam("key", "nonexistingKey")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());

  }

  @Test
  public void getAllCollections(){

    generateBuckets(bucketName);
    String versionChecksumObj1 = createObject(bucketName, objectName1, contentType1);
    String versionChecksumObj2 = createObject(bucketName, objectName2, contentType2);
    InputObject object1 = new InputObject(objectName1,versionChecksumObj1);
    InputObject object2 = new InputObject(objectName2,versionChecksumObj2);

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("collection1");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object1, object2}));
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertEquals(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    inputCollection.setKey("collection2");
    assertEquals(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    Response response = target("/collections").queryParam("bucketName", bucketName)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getHeaderString("Content-Type"), "application/json");

    JsonArray responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonArray();
    assertNotNull(responseObj);
    assertEquals(2, responseObj.size());
    Iterator<JsonElement> iterator = responseObj.iterator();
    JsonObject next = iterator.next().getAsJsonObject();
    assertEquals("collection1", next.get("key").getAsString());
    next = iterator.next().getAsJsonObject();
    assertEquals("collection2", next.get("key").getAsString());

  }

  @Test
  public void getCollectionsWithPagination(){

    generateBuckets(bucketName);
    String versionChecksumObj1 = createObject(bucketName, objectName1, contentType1);
    String versionChecksumObj2 = createObject(bucketName, objectName2, contentType2);

    InputObject object1 = new InputObject(objectName1,versionChecksumObj1);
    InputObject object2 = new InputObject(objectName2,versionChecksumObj2);

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("collection1");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object1, object2}));
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertEquals(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    inputCollection.setKey("collection2");
    assertEquals(target("/collections")
            .request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    Response response = target("/collections")
        .queryParam("bucketName", bucketName)
        .queryParam("offset", "1")
        .queryParam("limit", "1")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getHeaderString("Content-Type"), "application/json");

    JsonArray responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonArray();
    assertNotNull(responseObj);
    assertEquals(1, responseObj.size());
    Iterator<JsonElement> iterator = responseObj.iterator();
    JsonObject next = iterator.next().getAsJsonObject();
    assertEquals("collection2", next.get("key").getAsString());

  }

  @Test
  public void getCollectionsInvalidPagination(){

    assertRepoError(target("/collections")
        .queryParam("bucketName", bucketName)
        .queryParam("offset", "-1")
        .queryParam("limit", "1")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get(),
        Response.Status.BAD_REQUEST, RepoException.Type.InvalidOffset
    );

    assertRepoError(target("/collections")
            .queryParam("bucketName", bucketName)
            .queryParam("offset", "1")
            .queryParam("limit", "100000")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .get(),
        Response.Status.BAD_REQUEST, RepoException.Type.InvalidLimit
    );

    assertRepoError(target("/collections")
            .queryParam("bucketName", bucketName)
            .queryParam("offset", "1")
            .queryParam("limit", "-1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .get(),
        Response.Status.BAD_REQUEST, RepoException.Type.InvalidLimit
    );

  }

  @Test
  public void getCollectionsUsingTag(){

    generateBuckets(bucketName);
    String versionChecksumObj1 = createObject(bucketName, objectName1, contentType1);
    String versionChecksumObj2 = createObject(bucketName, objectName2, contentType2);

    InputObject object1 = new InputObject(objectName1,versionChecksumObj1);
    InputObject object2 = new InputObject(objectName2,versionChecksumObj2);

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("collection1");
    inputCollection.setCreate("new");
    inputCollection.setTag("AOP");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object1, object2}));
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertEquals(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    inputCollection.setKey("collection2");
    inputCollection.setTag("FINAL");
    assertEquals(target("/collections")
            .request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    Response response = target("/collections")
        .queryParam("bucketName", bucketName)
        .queryParam("tag", "FINAL")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getHeaderString("Content-Type"), "application/json");

    JsonArray responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonArray();
    assertNotNull(responseObj);
    assertEquals(1, responseObj.size());
    Iterator<JsonElement> iterator = responseObj.iterator();
    JsonObject next = iterator.next().getAsJsonObject();
    assertEquals("collection2", next.get("key").getAsString());

  }

  @Test
  public void getCollectionsUsingBucket(){

    generateBuckets(bucketName);
    String versionChecksumObj1 = createObject(bucketName, objectName1, contentType1);
    String versionChecksumObj2 = createObject(bucketName, objectName2, contentType2);

    InputObject object1 = new InputObject(objectName1,versionChecksumObj1);
    InputObject object2 = new InputObject(objectName2,versionChecksumObj2);

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("collection1");
    inputCollection.setCreate("new");
    inputCollection.setTag("AOP");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object1, object2}));
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertEquals(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    generateBuckets("bucket2");
    String versionChecksumObj3 = createObject("bucket2", "object3", "image/gif");

    InputCollection inputCollection2 = new InputCollection();
    inputCollection2.setBucketName("bucket2");
    inputCollection2.setKey("collection2");
    inputCollection2.setCreate("new");
    inputCollection2.setTag("AOP");
    inputCollection2.setObjects(Arrays.asList(new InputObject[]{new InputObject("object3", versionChecksumObj3)}));
    Entity<InputCollection> collectionEntity2 = Entity.entity(inputCollection2, MediaType.APPLICATION_JSON_TYPE);

    assertEquals(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity2).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    Response response = target("/collections")
        .queryParam("bucketName", "bucket2")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getHeaderString("Content-Type"), "application/json");

    JsonArray responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonArray();
    assertNotNull(responseObj);
    assertEquals(1, responseObj.size());
    Iterator<JsonElement> iterator = responseObj.iterator();
    JsonObject next = iterator.next().getAsJsonObject();
    assertEquals("collection2", next.get("key").getAsString());

  }

  @Test
  public void getCollectionUsingVersion(){

    generateCollectionData();

    Response response = target("/collections/" + bucketName)
        .queryParam("version", "0")
        .queryParam("key", "collection1")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getHeaderString("Content-Type"), "application/json");

    JsonObject responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonObject();
    assertNotNull(responseObj);
    assertEquals("collection1", responseObj.get("key").getAsString());
    assertEquals(0, responseObj.get("versionNumber").getAsInt());
    assertEquals(1, responseObj.get("objects").getAsJsonArray().size());

  }


  @Test
  public void getCollectionNoKey(){

    assertRepoError(target("/collections/" + bucketName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .get(),
        Response.Status.BAD_REQUEST, RepoException.Type.NoCollectionKeyEntered
    );

  }

  @Test
  /**
   * Test get collection using tag. It there is more than one collection with the same tag,
   * it should get the latest one.
   */
  public void getCollectionMoreThanOneWithSameTag(){

    generateBuckets(bucketName);
    String versionChecksumObj1 = createObject(bucketName, objectName1, contentType1);
    String versionChecksumObj2 = createObject(bucketName, objectName2, contentType2);

    InputObject object1 = new InputObject(objectName1,versionChecksumObj1);
    InputObject object2 = new InputObject(objectName2,versionChecksumObj2);

    // create collection 1
    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("collection1");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object1}));
    inputCollection.setTag("AOP");
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    // create collection1
    Response response = target("/collections").request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(collectionEntity);

    assertEquals(response.getStatus(),
        Response.Status.CREATED.getStatusCode());

    // version collection1, using a creation_date_time previous for the creation date time of collection 1
    inputCollection.setCreate("version");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object2}));
    inputCollection.setCreationDateTime(CREATION_DATE_TIME_STRING);

    response = target("/collections").request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(collectionEntity);

    assertEquals(response.getStatus(),
        Response.Status.CREATED.getStatusCode());

    response = target("/collections/" + bucketName)
        .queryParam("key", "collection1")
        .queryParam("tag", "AOP")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getHeaderString("Content-Type"), "application/json");

    JsonObject responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonObject();
    assertNotNull(responseObj);
    assertEquals("collection1", responseObj.get("key").getAsString());
    assertEquals(0, responseObj.get("versionNumber").getAsInt());

  }


  @Test
  /**
   * Test get latest collection. The first created collection has later creation date than the second one
   */
  public void getLatestCollection(){

    generateBuckets(bucketName);
    String versionChecksumObj1 = createObject(bucketName, objectName1, contentType1);
    String versionChecksumObj2 = createObject(bucketName, objectName2, contentType2);

    InputObject object1 = new InputObject(objectName1,versionChecksumObj1);
    InputObject object2 = new InputObject(objectName2,versionChecksumObj2);

    // create collection 1
    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("collection1");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object1}));
    inputCollection.setTag("AOP");
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    // create collection1
    Response response = target("/collections").request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(collectionEntity);

    assertEquals(response.getStatus(),
        Response.Status.CREATED.getStatusCode());

    // version collection1, using a creation_date_time previous for the creation date time of collection 1
    inputCollection.setCreate("version");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object2}));
    inputCollection.setCreationDateTime(CREATION_DATE_TIME_STRING);

    response = target("/collections").request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(collectionEntity);

    assertEquals(response.getStatus(),
        Response.Status.CREATED.getStatusCode());

    response = target("/collections/" + bucketName)
        .queryParam("key", "collection1")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getHeaderString("Content-Type"), "application/json");

    JsonObject responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonObject();
    assertNotNull(responseObj);
    assertEquals("collection1", responseObj.get("key").getAsString());
    assertEquals(0, responseObj.get("versionNumber").getAsInt());

  }

  private void generateBuckets(String bucketName){

    // create needed data
    target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.form(new Form()
            .param("name", bucketName)
            .param("creationDateTime",CREATION_DATE_TIME_STRING)));

  }

  private String createObject(String bucketName, String objectName1, String contentType){

    Response response = target("/objects").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "new")
                    .field("key", objectName1).field("contentType", contentType)
                    .field("timestamp", "2012-09-08 11:00:00")
                    .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA)
        );

    JsonObject responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonObject();
    assertNotNull(responseObj);
    return responseObj.get("versionChecksum").getAsString();

  }

  private void generateCollectionData(){

    generateBuckets(bucketName);
    String versionChecksumObj1 = createObject(bucketName, objectName1, contentType1);
    String versionChecksumObj2 = createObject(bucketName, objectName2, contentType2);

    InputObject object1 = new InputObject(objectName1,versionChecksumObj1);
    InputObject object2 = new InputObject(objectName2,versionChecksumObj2);

    // create collection 1
    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("collection1");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object1}));
    inputCollection.setTag("AOP");
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    Response response = target("/collections").request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(collectionEntity);

    assertEquals(response.getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    // version collection 1
    inputCollection.setCreate("version");
    inputCollection.setTag("FINAL");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object1, object2}));

    assertEquals(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

  }

  @Test
  public void getCollectionByTag(){

    generateCollectionData();

    Response response = target("/collections/" + bucketName)
        .queryParam("key", "collection1")
        .queryParam("tag", "AOP")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getHeaderString("Content-Type"), "application/json");

    JsonObject responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonObject();
    assertNotNull(responseObj);
    assertEquals("collection1", responseObj.get("key").getAsString());
    assertEquals(0, responseObj.get("versionNumber").getAsInt());
    assertEquals(1, responseObj.get("objects").getAsJsonArray().size());

  }

  @Test
  public void getCollectionByTagAndVersion(){

    generateCollectionData();

    Response response = target("/collections/" + bucketName)
        .queryParam("key", "collection1")
        .queryParam("tag", "AOP")
        .queryParam("version", "0")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getHeaderString("Content-Type"), "application/json");

    JsonObject responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonObject();
    assertNotNull(responseObj);
    assertEquals("collection1", responseObj.get("key").getAsString());
    assertEquals(0, responseObj.get("versionNumber").getAsInt());
    assertEquals(1, responseObj.get("objects").getAsJsonArray().size());

    assertRepoError(target("/collections/" + bucketName)
            .queryParam("key", "collection1")
            .queryParam("tag", "AOP")
            .queryParam("version", "1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .get(),
        Response.Status.NOT_FOUND, RepoException.Type.CollectionNotFound
    );

  }

  @Test
  public void getCollectionByVersionChecksum(){

    generateBuckets(bucketName);
    String versionChecksumObj1 = createObject(bucketName, objectName1, contentType1);

    InputObject object1 = new InputObject(objectName1,versionChecksumObj1);

    // create collection 1
    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("collection1");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object1}));
    inputCollection.setTag("AOP");
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    Response response = target("/collections").request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(collectionEntity);

    JsonObject responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonObject();
    assertNotNull(responseObj);
    String versionChecksum = responseObj.get("versionChecksum").getAsString();

    Response responseGet = target("/collections/" + bucketName)
        .queryParam("key", "collection1")
        .queryParam("versionChecksum", versionChecksum)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(responseGet.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(responseGet.getHeaderString("Content-Type"), "application/json");

    JsonObject responseObjGET = gson.fromJson(responseGet.readEntity(String.class), JsonElement.class).getAsJsonObject();
    assertNotNull(responseObjGET);
    assertEquals("collection1", responseObj.get("key").getAsString());
    assertEquals(0, responseObjGET.get("versionNumber").getAsInt());
    assertEquals(1, responseObjGET.get("objects").getAsJsonArray().size());
    assertEquals(versionChecksum, responseObjGET.get("versionChecksum").getAsString());


  }

  @Test
  public void deleteCollectionWithVersion(){

    generateCollectionData();

    assertEquals(target("/collections/" + bucketName)
            .queryParam("key", "collection1")
            .queryParam("version", 0)
            .request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .delete()
            .getStatus(),
        Response.Status.OK.getStatusCode()
    );

    assertRepoError(target("/collections/" + bucketName)
            .queryParam("key", "collection1")
            .queryParam("version", "0")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .get(),
        Response.Status.NOT_FOUND, RepoException.Type.CollectionNotFound
    );

  }

  @Test
  public void deleteCollectionWithVersionChecksum(){

    generateBuckets(bucketName);
    String versionChecksumObj1 = createObject(bucketName, objectName1, contentType1);

    InputObject object1 = new InputObject(objectName1,versionChecksumObj1);

    // create collection 1
    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("collection1");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object1}));
    inputCollection.setTag("AOP");
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    Response response = target("/collections").request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(collectionEntity);

    JsonObject responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonObject();
    assertNotNull(responseObj);
    String versionChecksum = responseObj.get("versionChecksum").getAsString();

    Response responseGet = target("/collections/" + bucketName)
        .queryParam("key", "collection1")
        .queryParam("tag", "AOP")
        .queryParam("version", "0")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(responseGet.getStatus(), Response.Status.OK.getStatusCode());

    assertEquals(target("/collections/" + bucketName)
            .queryParam("key", "collection1")
            .queryParam("versionChecksum", versionChecksum)
            .request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .delete()
            .getStatus(),
        Response.Status.OK.getStatusCode()
    );

    assertRepoError(target("/collections/" + bucketName)
            .queryParam("key", "collection1")
            .queryParam("version", "0")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .get(),
        Response.Status.NOT_FOUND, RepoException.Type.CollectionNotFound
    );

  }

  @Test
  public void deleteCollectionWithTag(){

    generateBuckets(bucketName);
    String versionChecksumObj1 = createObject(bucketName, objectName1, contentType1);

    InputObject object1 = new InputObject(objectName1,versionChecksumObj1);

    // create collection 1
    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("collection1");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object1}));
    inputCollection.setTag("AOP");
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    target("/collections").request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(collectionEntity);

    Response responseGet = target("/collections/" + bucketName)
        .queryParam("key", "collection1")
        .queryParam("tag", "AOP")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(responseGet.getStatus(), Response.Status.OK.getStatusCode());

    assertEquals(target("/collections/" + bucketName)
            .queryParam("key", "collection1")
            .queryParam("tag", "AOP")
            .request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .delete()
            .getStatus(),
        Response.Status.OK.getStatusCode()
    );

    assertRepoError(target("/collections/" + bucketName)
            .queryParam("key", "collection1")
            .queryParam("tag", "AOP")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .get(),
        Response.Status.NOT_FOUND, RepoException.Type.CollectionNotFound
    );

  }

  @Test
  public void deleteCollectionBadRequest(){

    assertRepoError(target("/collections/" + bucketName)
            .request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .delete(),
        Response.Status.BAD_REQUEST, RepoException.Type.NoCollectionKeyEntered
    );

    assertRepoError(target("/collections/" + bucketName)
            .queryParam("key", "collection1")
            .request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .delete(),
        Response.Status.BAD_REQUEST, RepoException.Type.NoFilterEntered
    );

  }

  @Test
  public void deleteCollectionNotFound(){

    assertRepoError(target("/collections/" + bucketName)
            .queryParam("key", "collection1")
            .queryParam("version", 1)
            .request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .delete(),
        Response.Status.NOT_FOUND, RepoException.Type.CollectionNotFound
    );

  }

  @Test
  public void deleteCollectionWithSameTags(){

    generateBuckets(bucketName);
    String versionChecksumObj1 = createObject(bucketName, objectName1, contentType1);

    InputObject object1 = new InputObject(objectName1,versionChecksumObj1);

    // create collection 1
    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("collection1");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object1}));
    inputCollection.setTag("AOP");
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    target("/collections").request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(collectionEntity);

    String versionChecksumObj2 = createObject(bucketName, objectName2, contentType2);
    InputObject object2 = new InputObject(objectName2,versionChecksumObj2);
    inputCollection.setCreate("version");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object2}));

    target("/collections").request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(collectionEntity);


    assertRepoError(target("/collections/" + bucketName)
            .queryParam("key", "collection1")
            .queryParam("tag", "AOP")
            .request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .delete(),
        Response.Status.BAD_REQUEST,
        RepoException.Type.MoreThanOneTaggedCollection);


  }

  @Test
  public void getCollectionsVersions(){

    generateBuckets(bucketName);
    String versionChecksumObj1 = createObject(bucketName, objectName1, contentType1);
    String versionChecksumObj2 = createObject(bucketName, objectName2, contentType2);

    InputObject object1 = new InputObject(objectName1,versionChecksumObj1);
    InputObject object2 = new InputObject(objectName2,versionChecksumObj2);

    // create collection1
    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("collection1");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object1, object2}));
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertEquals(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    // version collection1
    inputCollection.setKey("collection1");
    inputCollection.setCreate("version");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object2}));
    assertEquals(target("/collections")
            .request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    Response response = target("/collections/" + bucketName + "/versions")
        .queryParam("key", "collection1")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getHeaderString("Content-Type"), "application/json");

    JsonArray responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonArray();
    assertNotNull(responseObj);
    assertEquals(2, responseObj.size());

  }

  @Test
  public void getCollectionsVersionsNoKey() {

    assertRepoError(target("/collections/" + bucketName + "/versions")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .get(),
        Response.Status.BAD_REQUEST, RepoException.Type.NoCollectionKeyEntered);
  }

  @Test
  public void getCollectionsVersionsNoCollection() {

    assertRepoError(target("/collections/" + bucketName + "/versions")
            .queryParam("key", "collection1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .get(),
        Response.Status.NOT_FOUND, RepoException.Type.CollectionNotFound);
  }

}
