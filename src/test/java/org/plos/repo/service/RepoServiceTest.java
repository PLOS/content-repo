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

package org.plos.repo.service;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.plos.repo.TestSpringConfig;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.RepoObject;
import org.plos.repo.models.validator.TimestampInputValidator;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestSpringConfig.class)
public class RepoServiceTest {

  private static final String VALID_BUCKET = "bucket-1";
  private static final Integer VALID_OFFSET = 1;
  private static final Integer VALID_LIMIT = 100;
  private static final String VALID_TAG = "tag";

  @InjectMocks
  private RepoService repoService;

  @Mock
  private ObjectStore objectStore;

  @Mock
  private TimestampInputValidator timestampValidator;

  @Mock
  protected SqlService sqlService;

  @Mock
  private Bucket bucket;

  @Before
  public void setUp() {
    repoService = new RepoService();
    initMocks(this);
  }

  @Test
  public void testListObjectsHappyPath() throws RepoException, SQLException, MalformedURLException {
    doNothing().when(sqlService).getReadOnlyConnection();
    ;
    when(sqlService.getBucket(VALID_BUCKET)).thenReturn(bucket);

    List<RepoObject> repoObjects = new ArrayList<RepoObject>();
    RepoObject ob = new RepoObject();
    ob.setId(0);
    ob.setKey("key");
    repoObjects.add(ob);

    when(sqlService.listObjects(VALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, true, VALID_TAG)).thenReturn(repoObjects);

    when(objectStore.hasXReproxy()).thenReturn(true);

    URL[] urls = new URL[1];
    urls[0] = new URL("http://ut");

    when(objectStore.getRedirectURLs(eq(ob))).thenReturn(urls);

    doNothing().when(sqlService).releaseConnection();

    List<RepoObject> response = repoService.listObjects(VALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, true, VALID_TAG);

    assertNotNull(response);
    assertNotNull(response.get(0));
    assertNotNull(response.get(0).getReproxyURL());

    verify(sqlService).getReadOnlyConnection();
    verify(sqlService).getBucket(VALID_BUCKET);
    verify(sqlService).listObjects(VALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, true, VALID_TAG);
    verify(objectStore).hasXReproxy();
    verify(objectStore).getRedirectURLs(ob);
    verify(sqlService).releaseConnection();
  }

}
