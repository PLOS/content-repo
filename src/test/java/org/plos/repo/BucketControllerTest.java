package org.plos.repo;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.junit.Assert.assertEquals;

public class BucketControllerTest extends RepoBaseJerseyTest {

  @Before
  public void setup() throws Exception {
    RepoBaseSpringTest.clearData(objectStore, sqlService);
  }

  @Test
  public void testControllerCrud() throws Exception {

    // CREATE

    String responseString = target("/buckets").request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    assertEquals(responseString, "[]");

    Form form = new Form().param("name", "plos-bucketunittest-bucket1");
    Response response = target("/buckets").request(MediaType.APPLICATION_JSON_TYPE).post(Entity.form(form));
    assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());

    response = target("/buckets").request(MediaType.APPLICATION_JSON_TYPE).post(Entity.form(new Form().param("name", "plos-bucketunittest-bucket1")));
    assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

    response = target("/buckets").request(MediaType.APPLICATION_JSON_TYPE).post(Entity.form(new Form().param("name", "plos-bucketunittest-bucket2")));
    assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());

    response = target("/buckets").request(MediaType.APPLICATION_JSON_TYPE).post(Entity.form(new Form().param("name", "plos-bucketunittest-bad?&name")));
    assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());


    // LIST

    responseString = target("/buckets").request().accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    Gson gson = new Gson();
    JsonArray jsonArray = gson.fromJson(responseString, JsonElement.class).getAsJsonArray();
    assertEquals(jsonArray.size(), 2);


    // DELETE

    response = target("/buckets/plos-bucketunittest-bucket1").request().delete();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

    FormDataMultiPart formDataMultiPart = new FormDataMultiPart().field("bucketName", "plos-bucketunittest-bucket2").field("create", "new").field("key", "object1").field("file", "test", MediaType.TEXT_PLAIN_TYPE);
    response = target("/objects").request().post(Entity.entity(formDataMultiPart, MediaType.MULTIPART_FORM_DATA));
    //String r = response.readEntity(String.class);
    assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());

    response = target("/buckets/plos-bucketunittest-bucket2").request().delete();
    assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

    response = target("/buckets/plos-bucketunittest-bucket3").request().delete();
    assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());

  }

}
