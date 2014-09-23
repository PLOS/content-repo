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
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.junit.Before;
import org.junit.Test;
import org.plos.repo.service.RepoException;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ObjectControllerTest extends RepoBaseJerseyTest {

  private final String bucketName = "plos-objstoreunittest-bucket1";

  private final String testData1 = "test data one goes\nhere.";

  private final String testData2 = "test data two goes\nhere.";

  private final String CREATION_DATE_TIME = new Timestamp(new Date().getTime()).toString();

  @Before
  public void setup() throws Exception {
    RepoBaseSpringTest.clearData(objectStore, sqlService);
  }

  @Test
  public void createWithBadTimestamp() {
    assertRepoError(target("/objects").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "new")
                    .field("key", "badTimeStamp").field("timestamp", "abc")
                    .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA
            )),
        Response.Status.BAD_REQUEST, RepoException.Type.CouldNotParseTimestamp
        );
  }

  @Test
  public void createWithNoKey() {
    assertRepoError(target("/objects").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "new")
                    .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA
            )),
        Response.Status.BAD_REQUEST, RepoException.Type.NoKeyEntered
    );
  }

  @Test
  public void createWithNoBucket() {
    assertRepoError(target("/objects").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(new FormDataMultiPart()
                    .field("create", "new")
                    .field("key", "someKey")
                    .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA
            )),
        Response.Status.BAD_REQUEST, RepoException.Type.NoBucketEntered
    );
  }

  @Test
  public void createWithInvalidBucket() {
    assertRepoError(target("/objects").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", "bucket2")
                    .field("create", "new")
                    .field("key", "object1")
                    .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA
            )),
        Response.Status.NOT_FOUND, RepoException.Type.BucketNotFound
    );
  }

  @Test
  public void createWithNoCreateFlag() {
    assertRepoError(target("/objects").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName)
                    .field("key", "noCreateFlag")
                    .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA
            )),
        Response.Status.BAD_REQUEST, RepoException.Type.NoCreationMethodEntered
    );
  }

  @Test
  public void createWithExistingKey() {

    createBucket(bucketName, CREATION_DATE_TIME);

    assertEquals(target("/objects").request()
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "new")
                    .field("key", "object1").field("contentType", "text/plain")
                    .field("timestamp", "2012-09-08 11:00:00")
                    .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA
            )).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    assertRepoError(target("/objects").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "new")
                    .field("key", "object1").field("contentType", "text/plain")
                    .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA
            )),
        Response.Status.BAD_REQUEST, RepoException.Type.CantCreateNewObjectWithUsedKey
    );
  }

  @Test
  public void createWithEmptyData() {

    createBucket(bucketName, CREATION_DATE_TIME);

    assertRepoError(target("/objects").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "new")
                    .field("key", "emptyFile").field("contentType", "text/plain")
                    .field("file", "", MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA
            )),
        Response.Status.BAD_REQUEST, RepoException.Type.ObjectDataEmpty
    );
  }

  @Test
  public void createVersionWithoutOrig() {

    createBucket(bucketName, CREATION_DATE_TIME);

    assertRepoError(target("/objects").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "version")
                    .field("key", "object3").field("contentType", "text/something")
                    .field("downloadName", "object2.text")
                    .field("file", testData2, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA)),
        Response.Status.BAD_REQUEST, RepoException.Type.CantCreateVersionWithNoOrig);

  }

  @Test
  public void createWithInvalidCreateMethod() {

    createBucket(bucketName, CREATION_DATE_TIME);

    assertRepoError(target("/objects").request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "badrequest")
                    .field("key", "object2").field("contentType", "text/something")
                    .field("downloadName", "object2.text")
                    .field("file", testData2, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA)),
        Response.Status.BAD_REQUEST, RepoException.Type.InvalidCreationMethod);
  }

  @Test
  public void invalidOffset() {

    createBucket(bucketName, CREATION_DATE_TIME);

    assertRepoError(target("/objects").queryParam("bucketName", bucketName).queryParam("offset", "-1").request().accept(MediaType.APPLICATION_JSON_TYPE).get(), Response.Status.BAD_REQUEST, RepoException.Type.InvalidOffset);

  }

  @Test
  public void deleteWithErrors() {

    assertRepoError(target("/objects/" + bucketName).queryParam("version", "0").request().accept(MediaType.APPLICATION_JSON_TYPE).delete(), Response.Status.BAD_REQUEST, RepoException.Type.NoKeyEntered);

    assertRepoError(target("/objects/" + bucketName).queryParam("key", "object1").request().accept(MediaType.APPLICATION_JSON_TYPE).delete(), Response.Status.BAD_REQUEST, RepoException.Type.NoVersionEntered);

    assertRepoError(target("/objects/" + bucketName).queryParam("key", "object5").queryParam("version", "0").request().accept(MediaType.APPLICATION_JSON_TYPE).delete(), Response.Status.NOT_FOUND, RepoException.Type.ObjectNotFound);
  }

  @Test
  public void listNoBucket() {
    assertRepoError(target("/objects").queryParam("bucketName", "nonExistingBucket").request().accept(MediaType.APPLICATION_JSON_TYPE).get(), Response.Status.NOT_FOUND, RepoException.Type.BucketNotFound);
  }

  @Test
  public void offsetAndCount() {

    createBucket(bucketName, CREATION_DATE_TIME);

    int startCount = Integer.valueOf(
        gson.fromJson(target("/buckets/" + bucketName).request(MediaType.APPLICATION_JSON_TYPE).get(String.class),
            JsonElement.class).getAsJsonObject().get("activeObjects").toString());

    int count = 100;
    for (int i=0; i<count; ++i) {
      assertEquals(target("/objects").request()
              .post(Entity.entity(new FormDataMultiPart()
                      .field("bucketName", bucketName).field("create", "new")
                      .field("key", "count" + (i < 10 ? "0" : "") + i).field("contentType", "text/plain")
                      .field("timestamp", "2012-09-08 11:00:00")
                      .field("file", "value" + i, MediaType.TEXT_PLAIN_TYPE),
                  MediaType.MULTIPART_FORM_DATA
              )).getStatus(),
          Response.Status.CREATED.getStatusCode()
      );
    }

    // delete all even keys
    for (int i=0; i<count; i += 2) {
      Response response = target("/objects/" + bucketName).queryParam("key", "count" + (i < 10 ? "0" : "") + i).queryParam("version", "0").request().delete();
      assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    }

    int endCountTotal = Integer.valueOf(
        gson.fromJson(target("/buckets/" + bucketName).request(MediaType.APPLICATION_JSON_TYPE).get(String.class),
            JsonElement.class).getAsJsonObject().get("totalObjects").toString());

    assertEquals(endCountTotal-startCount, count);

    int endCountActive = Integer.valueOf(
        gson.fromJson(target("/buckets/" + bucketName).request(MediaType.APPLICATION_JSON_TYPE).get(String.class),
            JsonElement.class).getAsJsonObject().get("activeObjects").toString());

    assertEquals(endCountActive-startCount, count/2); // half of them are deleted, and not counted

    int subset = 10;
    String responseString = target("/objects").queryParam("limit", "" + subset).queryParam("offset", startCount + "").queryParam("includeDeleted", "true").request().accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    JsonArray jsonArray = gson.fromJson(responseString, JsonElement.class).getAsJsonArray();
    assertEquals(jsonArray.size(), subset);
    for (int i=0; i<subset; ++i) {
      assertEquals(jsonArray.get(i).getAsJsonObject().get("key").getAsString(), "count" + (i < 10 ? "0" : "") + i);
    }

    responseString = target("/objects").queryParam("bucketName", bucketName).queryParam("limit", "" + subset).queryParam("offset", 10).request().accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    jsonArray = gson.fromJson(responseString, JsonElement.class).getAsJsonArray();
    // response is alphabetical by key, so countNN is before any other
    assertEquals(jsonArray.size(), subset);
    for (int i=0, j=21; i<subset; ++i, j+=2) {
      assertEquals(jsonArray.get(i).getAsJsonObject().get("key").getAsString(), "count" + (j < 10 ? "0" : "") + j);
    }

  }

  /*@Test*/
  // TODO : rewrite test to include the new changes
  public void crudHappyPath() throws Exception {

    String responseString = target("/objects").request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    assertEquals(responseString, "[]");

    Form form = new Form().param("name", bucketName);
    Response response = target("/buckets").request(MediaType.APPLICATION_JSON_TYPE).post(Entity.form(form));
    assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());


    // CREATE

    assertEquals(target("/objects").request()
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "new")
                    .field("key", "object1").field("contentType", "text/plain")
                    .field("timestamp", "2012-09-08 11:00:00")
                    .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA
            )).getStatus(),
        Response.Status.CREATED.getStatusCode()
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
                    .field("bucketName", bucketName).field("create", "new")
                    .field("key", "funky&?#key").field("contentType", "text/plain")
                    .field("file", "object1", MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA
            )).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    assertEquals(target("/objects").request()
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "new")
                    .field("key", "emptyContentType")
                    .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA
            )).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    // TODO: create with empty file, update with empty file

    // TODO: create the same object in two buckets, and make sure deleting one does not delete the other




    // LIST

    responseString = target("/objects/").queryParam("bucketName", bucketName).request().accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    JsonArray jsonArray = gson.fromJson(responseString, JsonElement.class).getAsJsonArray();

    assertEquals(jsonArray.size(), 4);


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
    assertEquals(response.readEntity(String.class), "object1");
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getHeaderString("Content-Type"), "text/plain");
    assertEquals(response.getHeaderString("Content-Disposition"), "inline; filename=funky%26%3F%23key");

    response = target("/objects/" + bucketName).queryParam("key", "emptyContentType").request().get();
//    assertEquals(response.readEntity(String.class), "object1");
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getHeaderString("Content-Type"), "application/octet-stream");
//    assertEquals(response.getHeaderString("Content-Disposition"), "inline; filename=funky%26%3F%23key");

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
                    .field("key", "object2").field("contentType", "text/something")
                    .field("downloadName", "object2.text")
                    .field("file", testData2, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA)).getStatus(),
        Response.Status.CREATED.getStatusCode());

    assertEquals(target("/objects").request()
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "version")
                    .field("key", "object2").field("contentType", "text/something")
                    .field("downloadName", "object2.text")
                    .field("file", "", MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA)).getStatus(),
        Response.Status.CREATED.getStatusCode());


    // VERSION LIST

    responseString = target("/objects/" + bucketName).queryParam("key", "object2").queryParam("fetchMetadata", "true").request().accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    JsonObject jsonObject = gson.fromJson(responseString, JsonElement.class).getAsJsonObject();
    jsonArray = jsonObject.getAsJsonArray("versions");

    assertEquals(jsonArray.size(), 2);


    // AUTOCREATE

    assertEquals(target("/objects").request()
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "auto")
                    .field("key", "object2").field("contentType", "text/something")
                    .field("downloadName", "object2.text")
                    .field("file", testData2, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA)).getStatus(),
        Response.Status.CREATED.getStatusCode());

    assertEquals(target("/objects").request()
            .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "auto")
                    .field("key", "object4")
                    .field("file", testData2, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA)).getStatus(),
        Response.Status.CREATED.getStatusCode());


    // VERSION LIST

    responseString = target("/objects/" + bucketName).queryParam("key", "object2").queryParam("fetchMetadata", "true").request().accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    jsonObject = gson.fromJson(responseString, JsonElement.class).getAsJsonObject();
    jsonArray = jsonObject.getAsJsonArray("versions");

    assertEquals(jsonArray.size(), 4);



    // NEW VERSIONS METHOD
    
    responseString = target("/objects/meta/" + bucketName).queryParam("key", "object2").request().accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    jsonObject = gson.fromJson(responseString, JsonElement.class).getAsJsonObject();
    jsonArray = jsonObject.getAsJsonArray("versions");

    assertEquals(jsonArray.size(), 4);


    // DELETE

    response = target("/objects/" + bucketName).queryParam("key", "object1").queryParam("version", "0").request().delete();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

    // TODO: tests to add
    //   object deduplication
    //   check url redirect resolve order (db vs filestore)

  }

  private void createBucket(String bucketName, String creationDateTime){
    target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.form(new Form()
            .param("name", bucketName)
            .param("creationDateTime",creationDateTime)));
  }

}
