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
import org.plos.repo.models.InputCollection;
import org.plos.repo.models.InputObject;
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
  private final String objectName2 = "object2";

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
    inputCollection.setTimestampString("abc");
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
    inputCollection.setObjects(Arrays.asList(new InputObject[]{new InputObject("key", 0)}));
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertRepoError(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity),
        Response.Status.NOT_FOUND, RepoException.Type.BucketNotFound
    );
  }

  @Test
  public void createWithExistingKey(){

    generateBucketsAndObjects(bucketName, objectName1, objectName2);

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("existingKey");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{new InputObject(objectName1, 0)}));
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

    generateBucketsAndObjects(bucketName, objectName1, objectName2);

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("existingKey");
    inputCollection.setCreate("version");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{new InputObject(objectName1, 0)}));
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

    generateBucketsAndObjects(bucketName, objectName1, objectName2);

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("existingKey");
    inputCollection.setCreate("auto");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{new InputObject(objectName1, 0)}));
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertEquals(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

  }

  @Test
  public void autoVersionExistingKeyCollection(){

    generateBucketsAndObjects(bucketName, objectName1, objectName2);

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("existingKey");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{new InputObject(objectName1, 0)}));
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

    generateBucketsAndObjects(bucketName, objectName1, objectName2);

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("nonexistingKey");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{new InputObject("nonexistingKey", 0)}));
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertRepoError(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity),
        Response.Status.NOT_FOUND,
        RepoException.Type.ObjectCollectionNotFound
    );

  }

  @Test
  public void getAllCollections(){

    generateBucketsAndObjects(bucketName, objectName1, objectName2);

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("collection1");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{new InputObject(objectName1, 0)}));
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

    generateBucketsAndObjects(bucketName, objectName1, objectName2);

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("collection1");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{new InputObject(objectName1, 0)}));
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

    generateBucketsAndObjects(bucketName, objectName1, objectName2);

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("collection1");
    inputCollection.setCreate("new");
    inputCollection.setTag("AOP");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{new InputObject(objectName1, 0)}));
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

    generateBucketsAndObjects(bucketName, objectName1, objectName2);

    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("collection1");
    inputCollection.setCreate("new");
    inputCollection.setTag("AOP");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{new InputObject(objectName1, 0)}));
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertEquals(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    generateBucketsAndObjects("bucket2", "object3", "object4");

    InputCollection inputCollection2 = new InputCollection();
    inputCollection2.setBucketName("bucket2");
    inputCollection2.setKey("collection2");
    inputCollection2.setCreate("new");
    inputCollection2.setTag("AOP");
    inputCollection2.setObjects(Arrays.asList(new InputObject[]{new InputObject("object3", 0)}));
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
  public void getLastCollection(){

    generateCollectionData();

    Response response = target("/collections/" + bucketName)
        .queryParam("key", "collection1")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getHeaderString("Content-Type"), "application/json");

    JsonObject responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonObject();
    assertNotNull(responseObj);
    assertEquals("collection1", responseObj.get("key").getAsString());
    assertEquals(1, responseObj.get("versionNumber").getAsInt());
    assertEquals(2, responseObj.get("objects").getAsJsonArray().size());

  }

  private void generateBucketsAndObjects(String bucketName, String objectName1, String objectName2){

    // create needed data
    target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.form(new Form()
            .param("name", bucketName)
            .param("creationDateTime",CREATION_DATE_TIME_STRING)));
    target("/objects").request()
        .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "new")
                    .field("key", objectName1).field("contentType", "text/plain")
                    .field("timestamp", "2012-09-08 11:00:00")
                    .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA)
        );

    target("/objects").request()
        .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "new")
                    .field("key", objectName2).field("contentType", "text/plain")
                    .field("timestamp", "2012-09-08 11:00:00")
                    .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA)
        );

  }

  private void generateCollectionData(){

    generateBucketsAndObjects(bucketName, objectName1, objectName2);

    // create collection 1
    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("collection1");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{new InputObject(objectName1,0)}));
    inputCollection.setTag("AOP");
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    assertEquals(target("/collections").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(collectionEntity).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    // version collection 1
    inputCollection.setCreate("version");
    inputCollection.setTag("FINAL");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{new InputObject(objectName1, 0),
        new InputObject(objectName2, 0) }));

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
  public void deleteCollection(){

    generateCollectionData();

    assertEquals(target("/collections/" + bucketName)
            .queryParam("key", "collection1")
            .queryParam("version", "0")
            .queryParam("bucketName", bucketName)
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
  public void deleteCollectionBadRequest(){

    assertRepoError(target("/collections/" + bucketName)
            .queryParam("version", "0")
            .queryParam("bucketName", bucketName)
            .request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .delete(),
        Response.Status.BAD_REQUEST, RepoException.Type.NoCollectionKeyEntered
    );

    assertRepoError(target("/collections/" + bucketName)
            .queryParam("key", "collection1")
            .queryParam("bucketName", bucketName)
            .request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .delete(),
        Response.Status.BAD_REQUEST, RepoException.Type.NoCollectionVersionEntered
    );

  }

  @Test
  public void deleteCollectionNotFound(){

    assertRepoError(target("/collections/" + bucketName)
            .queryParam("key", "collection1")
            .queryParam("version", "0")
            .request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .delete(),
        Response.Status.NOT_FOUND, RepoException.Type.CollectionNotFound
    );

    generateCollectionData();

    assertRepoError(target("/collections/" + bucketName)
            .queryParam("key", "inexColl")
            .queryParam("version", "0")
            .queryParam("bucketName", bucketName)
            .request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .delete(),
        Response.Status.NOT_FOUND, RepoException.Type.CollectionNotFound
    );

  }

}
