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
import junit.framework.TestCase;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.junit.Before;
import org.junit.Test;
import org.plos.repo.models.input.InputCollection;
import org.plos.repo.models.input.InputObject;
import org.plos.repo.service.RepoException;
import org.springframework.http.HttpStatus;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class BucketControllerTest extends RepoBaseJerseyTest {

  private final String bucketName = "plos-bucketunittest-bucket1";
  private final Timestamp CREATION_DATE_TIME = new Timestamp(new Date().getTime());
  private final String CREATION_DATE_TIME_STRING = CREATION_DATE_TIME.toString();

  @Before
  public void setup() throws Exception {
    RepoBaseSpringTest.clearData(objectStore, sqlService);
  }

  @Test
  public void bucketAlreadyExists() {
    target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.form(new Form()
            .param("name", bucketName)
            .param("creationDateTime",CREATION_DATE_TIME_STRING)));

    assertRepoError(
        target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.form(new Form()
                .param("name", bucketName)
                .param("creationDateTime",CREATION_DATE_TIME_STRING))),
        Response.Status.BAD_REQUEST, RepoException.Type.BucketAlreadyExists);
  }

  @Test
  public void invalidBucketName() {

    assertRepoError(
        target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.form(new Form()
                .param("name", "plos-bucketunittest-bad?&name")
                .param("creationDateTime",CREATION_DATE_TIME_STRING))),
        Response.Status.BAD_REQUEST, RepoException.Type.IllegalBucketName);
  }

  @Test
  public void deleteNonEmptyBucket() {

    target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.form(new Form()
            .param("name", bucketName)
            .param("creationDateTime",CREATION_DATE_TIME_STRING)));

    target("/objects").request().post(Entity.entity(new FormDataMultiPart().field("bucketName", bucketName).field("create", "new").field("key", "object1").field("file", "test", MediaType.TEXT_PLAIN_TYPE), MediaType.MULTIPART_FORM_DATA));

    assertRepoError(
        target("/buckets/" + bucketName)
            .request(MediaType.APPLICATION_JSON_TYPE).delete(),
        Response.Status.BAD_REQUEST, RepoException.Type.CantDeleteNonEmptyBucket);
  }

  @Test
  public void deleteNonExsitingBucket() {

    assertRepoError(
        target("/buckets/" + "nonExistingBucket")
            .request(MediaType.APPLICATION_JSON_TYPE).delete(),
        Response.Status.NOT_FOUND, RepoException.Type.BucketNotFound);
  }

  @Test
  public void crudHappyPath() throws Exception {

    // CREATE

    String responseString = target("/buckets").request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    assertEquals(responseString, "[]");

    Response response = target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.form(new Form().param("name", bucketName).param("creationDateTime",CREATION_DATE_TIME_STRING)));

    assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());

    response = target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.form(new Form()
            .param("name", "plos-bucketunittest-bucket2")
            .param("creationDateTime",CREATION_DATE_TIME_STRING)));

    assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());

    // LIST

    responseString = target("/buckets").request().accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    JsonArray jsonArray = gson.fromJson(responseString, JsonElement.class).getAsJsonArray();
    assertEquals(jsonArray.size(), 2);


    // DELETE

    response = target("/buckets/" + bucketName).request().delete();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

  }

  @Test
  public void deleteBucketWithDeletedContent(){

    // create bucket
    Response createBucketResponse = target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.form(new Form().param("name", bucketName).param("creationDateTime",CREATION_DATE_TIME_STRING)));

    assertEquals(createBucketResponse.getStatus(), Response.Status.CREATED.getStatusCode());

    // create object
    Response createObjResponse = target("/objects").request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(new FormDataMultiPart()
                .field("bucketName", bucketName).field("create", "new")
                .field("key", "object3").field("contentType", "text/something")
                .field("downloadName", "object3.text")
                .field("file", "testData2", MediaType.TEXT_PLAIN_TYPE),
            MediaType.MULTIPART_FORM_DATA
        ));

    JsonObject responseObj = gson.fromJson(createObjResponse.readEntity(String.class), JsonElement.class).getAsJsonObject();
    TestCase.assertNotNull(responseObj);
    String versionChecksum = responseObj.get("versionChecksum").getAsString();

    Response deleteObjectResponse = target("/objects/" + bucketName)
        .queryParam("key", "object3")
        .queryParam("versionChecksum", versionChecksum)
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .delete();

    Response deleteBucketResponse = target("/buckets/" + bucketName).request().accept(MediaType.APPLICATION_JSON_TYPE).delete();

    assertRepoError(deleteBucketResponse, Response.Status.BAD_REQUEST, RepoException.Type.CantDeleteNonEmptyBucket);

  }

  @Test
  public void deleteBucketWithPurgeContent(){

    Response createBucketResponse = target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.form(new Form().param("name", bucketName).param("creationDateTime",CREATION_DATE_TIME_STRING)));

    assertEquals(createBucketResponse.getStatus(), Response.Status.CREATED.getStatusCode());

    // create object
    Response createObjResponse = target("/objects").request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(new FormDataMultiPart()
                .field("bucketName", bucketName).field("create", "new")
                .field("key", "object3").field("contentType", "text/something")
                .field("downloadName", "object3.text")
                .field("file", "testData2", MediaType.TEXT_PLAIN_TYPE),
            MediaType.MULTIPART_FORM_DATA
        ));


    JsonObject responseObj = gson.fromJson(createObjResponse.readEntity(String.class), JsonElement.class).getAsJsonObject();
    TestCase.assertNotNull(responseObj);
    String versionChecksumObj = responseObj.get("versionChecksum").getAsString();

    InputObject object1 = new InputObject("object3", versionChecksumObj);

    // create collection
    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("collection1");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(new InputObject[]{object1}));
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    Response createCollResponse = target("/collections").request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(collectionEntity);

    JsonObject responseColl = gson.fromJson(createCollResponse.readEntity(String.class), JsonElement.class).getAsJsonObject();
    TestCase.assertNotNull(responseObj);
    String versionChecksumColl = responseObj.get("versionChecksum").getAsString();

    // purge object
    Response purgeResponse = target("/objects/" + bucketName)
        .queryParam("key", "object3")
        .queryParam("purge", true)
        .queryParam("versionChecksum", versionChecksumObj)
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .delete();

    // delete bucket
    Response deleteBucketResponse = target("/buckets/" + bucketName).request().accept(MediaType.APPLICATION_JSON_TYPE).delete();
    assertEquals(Response.Status.OK.getStatusCode(), deleteBucketResponse.getStatus());

    // get object
    Response getObjResponse = target("/objects/" + bucketName).queryParam("key", "object1").queryParam("versionChecksum", versionChecksumObj).request().get();
    assertRepoError(getObjResponse, Response.Status.NOT_FOUND, RepoException.Type.ObjectNotFound);

    // get object
    Response getColResponse = target("/collections/" + bucketName)
        .queryParam("key", "collection1")
        .queryParam("versionChecksum", versionChecksumColl)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertRepoError(getColResponse, Response.Status.NOT_FOUND, RepoException.Type.CollectionNotFound);

  }

}
