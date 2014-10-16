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


import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;

public class RootControllerTest extends RepoBaseJerseyTest {

  private static String BUCKET_NAME1 = "b1";
  private static String BUCKET_NAME2 = "b2";

  @Before
  public void setup() throws Exception {
    RepoBaseSpringTest.clearData(objectStore, sqlService);
  }

  @Test
  public void getStatusTest(){

    // create bucket 1
    target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.form(new Form()
            .param("name", BUCKET_NAME1)));

    // create bucket 2
    target("/buckets").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.form(new Form()
            .param("name", BUCKET_NAME2)));

    Response response = target("/status")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getHeaderString("Content-Type"), "application/json");

    JsonObject responseObj = gson.fromJson(response.readEntity(String.class), JsonElement.class).getAsJsonObject();
    assertNotNull(responseObj);
    assertEquals(2, responseObj.get("bucketCount").getAsInt());

    assertNotNull(responseObj.get("serviceStarted").getAsString());
    assertNotNull(responseObj.get("readsSinceStart").getAsLong());
    assertNotNull(responseObj.get("writesSinceStart").getAsLong());

  }


}
