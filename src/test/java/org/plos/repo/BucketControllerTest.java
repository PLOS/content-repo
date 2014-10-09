package org.plos.repo;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.junit.Before;
import org.junit.Test;
import org.plos.repo.service.RepoException;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.sql.Timestamp;
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

}
