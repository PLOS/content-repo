/*
 * Copyright (c) 2006-2014 by Public Library of Science
 *
 *    http://plos.org
 *    http://ambraproject.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.plos.repo;

import com.google.gson.Gson;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.plos.repo.models.RepoObject;
import org.plos.repo.rest.ObjectController;
import org.plos.repo.service.RepoException;
import org.plos.repo.service.RepoInfoService;
import org.plos.repo.service.RepoService;

import javax.ws.rs.core.Response;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ObjectControllerUTest{

  private static final String BUCKET = "bucket-1";
  private static final Integer OFFSET = 1;
  private static final Integer LIMIT = 100;
  private static final String TAG = "tag";

  private Timestamp timestamp = new Timestamp(new Date().getTime());

  @InjectMocks
  private ObjectController objectController;

  @Mock
  private RepoService repoService;

  @Mock
  private RepoInfoService repoInfoService;

  private Gson gson = new Gson();

  @Before
  public void setUp(){

    objectController = new ObjectController();
    initMocks(this);

  }


  @Test
  public void reproxyUrlGenerationTest() throws MalformedURLException, RepoException {

    RepoObject object1 = mock(RepoObject.class);
    URL url1 = new URL("http", "localhost", 8080, "contentRepo1");
    URL url2 = new URL("http", "localhost", 8080, "contentRepo2");
    when(object1.getReproxyURL()).thenReturn(new URL[]{url1, url2});
    when(object1.getTimestamp()).thenReturn(timestamp);
    when(object1.getCreationDate()).thenReturn(timestamp);

    RepoObject object2 = mock(RepoObject.class);
    when(object2.getReproxyURL()).thenReturn(new URL[]{});
    when(object2.getTimestamp()).thenReturn(timestamp);
    when(object2.getCreationDate()).thenReturn(timestamp);

    RepoObject object3 = mock(RepoObject.class);
    when(object3.getReproxyURL()).thenReturn(new URL[]{url2});
    when(object3.getTimestamp()).thenReturn(timestamp);
    when(object3.getCreationDate()).thenReturn(timestamp);

    RepoObject object4 = mock(RepoObject.class);
    when(object4.getReproxyURL()).thenReturn(null);
    when(object4.getTimestamp()).thenReturn(timestamp);
    when(object4.getCreationDate()).thenReturn(timestamp);

    List<RepoObject> repoObjects = new ArrayList<RepoObject>();
    repoObjects.add(object1);
    repoObjects.add(object2);
    repoObjects.add(object3);
    repoObjects.add(object4);

    when(repoService.listObjects(BUCKET, OFFSET, LIMIT, true, TAG)).thenReturn(repoObjects);

    Response objectsResponse = objectController.listObjects(BUCKET, OFFSET, LIMIT, true, TAG);

    assertNotNull(objectsResponse);
    assertEquals(objectsResponse.getStatus(), Response.Status.OK.getStatusCode());

  }

}
