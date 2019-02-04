/*
 * Copyright (c) 2014-2019 Public Library of Science
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
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.junit.Test;
import org.plos.repo.models.Operation;
import org.plos.repo.models.input.InputCollection;
import org.plos.repo.models.input.InputObject;

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

public class AuditControllerTest extends RepoBaseJerseyTest {

  private final String bucketName = "plos-objstoreunittest-bucket1";
  private final String objectName1 = "object1";
  private final String contentType1 = "text/plain";

  private final String objectName2 = "object2";
  private final String contentType2 = "image/jpg";

  private final Timestamp CREATION_DATE_TIME = new Timestamp(new Date().getTime());
  private final String CREATION_DATE_TIME_STRING = CREATION_DATE_TIME.toString();

  private final String testData1 = "test data one goes\nhere.";
  private String USER_METADATA = "{ \"key\": \"obj1\", \"versionChecksum\":\"dkasdny84923mkdnu914i21\", \"version\":1.1 }";

  @Test
  public void getAuditRecordsTest() {

    createElementsToAudit();

    // querying for all the audit records
    Response response = target("/audit")
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getHeaderString("Content-Type"), "application/json");

    JsonArray responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonArray();
    assertNotNull(responseObj);
    assertEquals(5, responseObj.size());

    // querying audit records using pagination
    response = target("/audit/")
        .queryParam("offset", 3)
        .queryParam("limit", 2)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getHeaderString("Content-Type"), "application/json");

    responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonArray();
    assertNotNull(responseObj);
    assertEquals(2, responseObj.size());

    Iterator<JsonElement> iterator = responseObj.iterator();
    JsonObject next = iterator.next().getAsJsonObject();
    assertEquals(Operation.CREATE_COLLECTION.getValue(), next.get("operation").getAsString());
    next = iterator.next().getAsJsonObject();
    assertEquals(Operation.DELETE_COLLECTION.getValue(), next.get("operation").getAsString());

  }

  private void createElementsToAudit() {

    // create bucket
    assertEquals(target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.form(new Form()
                .param("name", bucketName)))
            .getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    // create object
    Response response = target("/objects").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(new FormDataMultiPart()
                    .field("bucketName", bucketName).field("create", "new")
                    .field("key", "obj1")
                    .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
                MediaType.MULTIPART_FORM_DATA)
        );

    JsonObject responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonObject();
    String objectUuid = responseObj.get("uuid").getAsString();

    // version object
    assertEquals(target("/objects").request(MediaType.APPLICATION_JSON_TYPE)
            .post(Entity.entity(new FormDataMultiPart()
                        .field("bucketName", bucketName).field("create", "version")
                        .field("key", "obj1").field("contentType", "text/plain")
                        .field("file", testData1, MediaType.TEXT_PLAIN_TYPE),
                    MediaType.MULTIPART_FORM_DATA)
            ).getStatus(),
        Response.Status.CREATED.getStatusCode()
    );

    // create collection
    InputObject object1 = new InputObject("obj1", objectUuid);

    // create collection 1
    InputCollection inputCollection = new InputCollection();
    inputCollection.setBucketName(bucketName);
    inputCollection.setKey("collection1");
    inputCollection.setCreate("new");
    inputCollection.setObjects(Arrays.asList(object1));
    inputCollection.setTag("AOP");
    Entity<InputCollection> collectionEntity = Entity.entity(inputCollection, MediaType.APPLICATION_JSON_TYPE);

    response = target("/collections").request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(collectionEntity);

    responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonObject();
    String collectionUuid = responseObj.get("uuid").getAsString();


    // delete collection
    assertEquals(target("/collections/" + bucketName)
            .queryParam("key", "collection1")
            .queryParam("uuid", collectionUuid)
            .request()
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .delete()
            .getStatus(),
        Response.Status.OK.getStatusCode()
    );

  }

}