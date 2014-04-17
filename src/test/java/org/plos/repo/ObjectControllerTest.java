package org.plos.repo;

import com.google.gson.Gson;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class ObjectControllerTest extends RepoBaseTest {

  @Test
  public void testControllerCrud() {

    Gson gson = new Gson();

    String bucketName = "plos-objstoreunittest-bucket1";

    clearData();

    String responseString = target("/objects").request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    Assert.assertEquals(responseString, "[]");

    Form form = new Form().param("name", bucketName);
    Response response = target("/buckets").request(MediaType.APPLICATION_JSON_TYPE).post(Entity.form(form));
    Assert.assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());

    String testData1 = "test data one goes\nhere.";

    String testData2 = "test data two goes\nhere.";


    // CREATE

    Assert.assertEquals(target("/objects").request()
        .post(Entity.entity(new FormDataMultiPart()
                .field("bucketName",bucketName).field("create", "new")
                .field("key", "object1").field("contentType", "text/plain")
                .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
            MediaType.MULTIPART_FORM_DATA)).getStatus(),
        Response.Status.CREATED.getStatusCode());

    Assert.assertEquals(target("/objects").request()
        .post(Entity.entity(new FormDataMultiPart()
                .field("bucketName", bucketName).field("create", "new")
                .field("key", "object1").field("contentType", "text/plain")
                .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
            MediaType.MULTIPART_FORM_DATA)).getStatus(),
        Response.Status.CONFLICT.getStatusCode());

    Assert.assertEquals(target("/objects").request()
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "new")
                    .field("key", "object2").field("contentType", "text/something")
                    .field("downloadName", "object2.txt")
                    .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA)).getStatus(),
        Response.Status.CREATED.getStatusCode());

    Assert.assertEquals(target("/objects").request()
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", "bucket2").field("create", "new")
                    .field("key", "object1").field("contentType", "text/plain")
                    .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA)).getStatus(),
        Response.Status.PRECONDITION_FAILED.getStatusCode());

    Assert.assertEquals(target("/objects").request()
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "new")
                    .field("key", "funky&?#key").field("contentType", "text/plain")
                    .field("file", "", MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA)).getStatus(),
        Response.Status.CREATED.getStatusCode());


    // TODO: create the same object in two buckets, and make sure deleting one does not delete the other

    // READ



  }
}
