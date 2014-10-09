package org.plos.repo;


import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.junit.Before;
import org.junit.Test;
import org.plos.repo.models.input.InputCollection;
import org.plos.repo.models.input.InputObject;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RootControllerTest extends RepoBaseJerseyTest {

  private static final String OBJECT_NAME1 = "obj1";
  private static String BUCKET_NAME1 = "b1";
  private static String BUCKET_NAME2 = "b2";

  @Before
  public void setup() throws Exception {
    RepoBaseSpringTest.clearData(objectStore, sqlService);
  }

  @Test
  public void getCollectionsVersions(){

    // create bucket 1
    target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.form(new Form()
            .param("name", BUCKET_NAME1)));

    // create bucket 2
    target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.form(new Form()
            .param("name", BUCKET_NAME2)));

    target("/objects").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", BUCKET_NAME2)
                    .field("create", "new")
                    .field("key", OBJECT_NAME1)
                    .field("file", "testData", MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA)
        );


    Response response = target("/status")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getHeaderString("Content-Type"), "application/json");

    JsonObject responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonObject();
    assertNotNull(responseObj);
    assertEquals(2, responseObj.get("bucketCount").getAsInt());

    JsonArray bucketsSize = responseObj.getAsJsonArray("bucketsSize");
    assertEquals(2, bucketsSize.size());

    JsonObject b1Info = bucketsSize.get(0).getAsJsonObject();
    assertEquals(BUCKET_NAME1, b1Info.get("bucketName").getAsString());
    assertEquals(0l, b1Info.get("bytes").getAsInt());

    JsonObject b2Info = bucketsSize.get(1).getAsJsonObject();
    assertEquals(BUCKET_NAME2, b2Info.get("bucketName").getAsString());
    assertTrue(0 < b2Info.get("bytes").getAsInt());


  }


}
