package org.plos.repo;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ObjectControllerTest extends RepoBaseTest {

  @Test
  public void testControllerCrud() throws Exception {

    Gson gson = new Gson();

    String bucketName = "plos-objstoreunittest-bucket1";

    clearData();

    String responseString = target("/objects").request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    assertEquals(responseString, "[]");

    Form form = new Form().param("name", bucketName);
    Response response = target("/buckets").request(MediaType.APPLICATION_JSON_TYPE).post(Entity.form(form));
    assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());

    String testData1 = "test data one goes\nhere.";

    String testData2 = "test data two goes\nhere.";


    // CREATE

    assertEquals(target("/objects").request()
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "new")
                    .field("key", "object1").field("contentType", "text/plain")
                    .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA
            )).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    assertEquals(target("/objects").request()
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "new")
                    .field("key", "object1").field("contentType", "text/plain")
                    .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA
            )).getStatus(),
        Response.Status.CONFLICT.getStatusCode()
    );

    assertEquals(target("/objects").request()
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "new")
                    .field("key", "object2").field("contentType", "text/something")
                    .field("downloadName", "object2.text")
                    .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA
            )).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    assertEquals(target("/objects").request()
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", "bucket2").field("create", "new")
                    .field("key", "object1").field("contentType", "text/plain")
                    .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA
            )).getStatus(),
        Response.Status.PRECONDITION_FAILED.getStatusCode()
    );

    assertEquals(target("/objects").request()
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "new")
                    .field("key", "funky&?#key").field("contentType", "text/plain")
                    .field("file", "", MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA
            )).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );


    // TODO: create the same object in two buckets, and make sure deleting one does not delete the other


    // READ

    response = target("/objects/" + bucketName).queryParam("key", "object1").request().get();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.readEntity(String.class), testData1);
    assertEquals(response.getHeaderString("Content-Type"), "text/plain");
    assertEquals(response.getHeaderString("Content-Disposition"), "inline; filename=object1");

    response = target("/objects/" + bucketName).queryParam("key", "object1").queryParam("version", "0").request().get();
    assertEquals(response.readEntity(String.class), testData1);
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getHeaderString("Content-Type"), "text/plain");
    assertEquals(response.getHeaderString("Content-Disposition"), "inline; filename=object1");

    response = target("/objects/" + bucketName).queryParam("key", "object1").queryParam("version", "10").request().get();
    assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());

    response = target("/objects/" + bucketName).queryParam("key", "object2").request().get();
    assertEquals(response.readEntity(String.class), testData1);
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getHeaderString("Content-Type"), "text/something");
    assertEquals(response.getHeaderString("Content-Disposition"), "inline; filename=object2.text");

    response = target("/objects/" + bucketName).queryParam("key", "funky&?#key").request().get();
    assertEquals(response.readEntity(String.class), "");
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getHeaderString("Content-Type"), "text/plain");
    assertEquals(response.getHeaderString("Content-Disposition"), "inline; filename=funky%26%3F%23key");

    response = target("/objects/" + bucketName).queryParam("key", "notObject").request().get();
    assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());


    // REPROXY

    if (objectStore.hasXReproxy()) {
      response = target("/objects/" + bucketName).queryParam("key", "object1").queryParam("version", "0").request().header("X-Proxy-Capabilities", "reproxy-file").get();
      assertNotNull(response.getHeaderString("X-Reproxy-URL"));
      assertEquals(response.readEntity(String.class), "");
      assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

      String reproxyUrl = response.getHeaderString("X-Reproxy-URL");
      URL url = new URL(reproxyUrl);
      InputStream inputStream = url.openStream();
      String testData1Out = IOUtils.toString(url.openStream(), Charset.defaultCharset());
      inputStream.close();
      assertEquals(testData1, testData1Out);
    }


    // CREATE NEW VERSION

    assertEquals(target("/objects").request()
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "version")
                    .field("key", "object3").field("contentType", "text/something")
                    .field("downloadName", "object2.text")
                    .field("file", testData2, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA)).getStatus(),
        Response.Status.NOT_ACCEPTABLE.getStatusCode());

    assertEquals(target("/objects").request()
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "version")
                    .field("key", "object2").field("contentType", "text/something")
                    .field("downloadName", "object2.text")
                    .field("file", testData2, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA)).getStatus(),
        Response.Status.OK.getStatusCode());


    // AUTOCREATE

    assertEquals(target("/objects").request()
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "auto")
                    .field("key", "object2").field("contentType", "text/something")
                    .field("downloadName", "object2.text")
                    .field("file", testData2, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA)).getStatus(),
        Response.Status.OK.getStatusCode());

    assertEquals(target("/objects").request()
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "badrequest")
                    .field("key", "object2").field("contentType", "text/something")
                    .field("downloadName", "object2.text")
                    .field("file", testData2, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA)).getStatus(),
        Response.Status.BAD_REQUEST.getStatusCode());

    assertEquals(target("/objects").request()
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "auto")
                    .field("key", "object4")
                    .field("file", testData2, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA)).getStatus(),
        Response.Status.CREATED.getStatusCode());


    // VERSION LIST

    responseString = target("/objects/" + bucketName).queryParam("key", "object2").queryParam("fetchMetadata", "true").request().accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    JsonObject jsonObject = gson.fromJson(responseString, JsonElement.class).getAsJsonObject();
    JsonArray jsonArray = jsonObject.getAsJsonArray("versions");

    assertEquals(jsonArray.size(), 3);


    // DELETE

    response = target("/objects/" + bucketName).queryParam("key", "object1").queryParam("version", "0").request().delete();

    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());


    // TODO: tests to add
    //   object deduplication
    //   check url redirect resolve order (db vs filestore)

  }
}
