/*
 * Copyright (c) 2017 Public Library of Science
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

package org.plos.repo;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import junit.framework.TestCase;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
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

import static org.junit.Assert.assertEquals;

public class BucketControllerTest extends RepoBaseJerseyTest {

  private final String bucketName = "plos-bucketunittest-bucket1";
  private final Timestamp CREATION_DATE_TIME = new Timestamp(new Date().getTime());
  private final String CREATION_DATE_TIME_STRING = CREATION_DATE_TIME.toString();

  @Test
  public void bucketAlreadyExists() {
    target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.form(new Form()
            .param("name", bucketName)
            .param("creationDateTime", CREATION_DATE_TIME_STRING)));

    assertRepoError(
        target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.form(new Form()
                .param("name", bucketName)
                .param("creationDateTime", CREATION_DATE_TIME_STRING))),
        Response.Status.BAD_REQUEST, RepoException.Type.BucketAlreadyExists);
  }

  @Test
  public void invalidBucketName() {
    assertRepoError(
        target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.form(new Form()
                .param("name", "plos-bucketunittest-bad?&name")
                .param("creationDateTime", CREATION_DATE_TIME_STRING))),
        Response.Status.BAD_REQUEST, RepoException.Type.IllegalBucketName);
  }

  @Test
  public void deleteNonEmptyBucket() {
    target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.form(new Form()
            .param("name", bucketName)
            .param("creationDateTime", CREATION_DATE_TIME_STRING)));

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
        .post(Entity.form(new Form().param("name", bucketName).param("creationDateTime", CREATION_DATE_TIME_STRING)));

    assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());

    response = target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.form(new Form()
            .param("name", "plos-bucketunittest-bucket2")
            .param("creationDateTime", CREATION_DATE_TIME_STRING)));

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
  public void deleteBucketWithDeletedContent() {
    // create bucket
    Response createBucketResponse = target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.form(new Form().param("name", bucketName).param("creationDateTime", CREATION_DATE_TIME_STRING)));

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
    String uuid = responseObj.get("uuid").getAsString();

    Response deleteObjectResponse = target("/objects/" + bucketName)
        .queryParam("key", "object3")
        .queryParam("uuid", uuid)
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .delete();

    Response deleteBucketResponse = target("/buckets/" + bucketName).request().accept(MediaType.APPLICATION_JSON_TYPE).delete();

    assertRepoError(deleteBucketResponse, Response.Status.BAD_REQUEST, RepoException.Type.CantDeleteNonEmptyBucket);
  }

  @Test
  public void deleteBucketWithPurgeContent() {
    Response createBucketResponse = target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.form(new Form().param("name", bucketName).param("creationDateTime", CREATION_DATE_TIME_STRING)));

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
    String uuidObj = responseObj.get("uuid").getAsString();

    InputObject object1 = new InputObject("object3", uuidObj);

    // create collection
    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("collection1");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(object1));
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    Response createCollResponse = target("/collections").request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(collectionEntity);

    JsonObject responseColl = gson.fromJson(createCollResponse.readEntity(String.class), JsonElement.class).getAsJsonObject();
    TestCase.assertNotNull(responseObj);
    String uuidCol = responseObj.get("uuid").getAsString();

    // purge object
    Response purgeResponse = target("/objects/" + bucketName)
        .queryParam("key", "object3")
        .queryParam("purge", true)
        .queryParam("uuid", uuidObj)
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .delete();

    // delete bucket
    Response deleteBucketResponse = target("/buckets/" + bucketName).request().accept(MediaType.APPLICATION_JSON_TYPE).delete();
    assertEquals(Response.Status.OK.getStatusCode(), deleteBucketResponse.getStatus());

    // get object
    Response getObjResponse = target("/objects/" + bucketName).queryParam("key", "object1").queryParam("uuid", uuidObj).request().get();
    assertRepoError(getObjResponse, Response.Status.NOT_FOUND, RepoException.Type.ObjectNotFound);

    // get object
    Response getColResponse = target("/collections/" + bucketName)
        .queryParam("key", "collection1")
        .queryParam("uuid", uuidCol)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertRepoError(getColResponse, Response.Status.NOT_FOUND, RepoException.Type.CollectionNotFound);
  }

}
