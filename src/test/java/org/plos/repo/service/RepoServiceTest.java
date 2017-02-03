/*
 * Copyright (c) 2017 Public Library of Science
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
    when(sqlService.getBucket(VALID_BUCKET)).thenReturn(bucket);

    List<RepoObject> repoObjects = new ArrayList<>();
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
