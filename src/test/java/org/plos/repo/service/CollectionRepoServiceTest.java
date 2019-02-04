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

package org.plos.repo.service;


import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.RepoCollection;
import org.plos.repo.models.input.ElementFilter;
import org.plos.repo.models.input.InputCollection;
import org.plos.repo.models.validator.InputCollectionValidator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class CollectionRepoServiceTest {

  private static final String VALID_BUCKET = "bucket-1";
  private static final String INVALID_BUCKET = "invalid-bucket";
  private static final Integer VALID_OFFSET = 1;
  private static final Integer VALID_LIMIT = 100;
  private static final String VALID_TAG = "tag";
  private static final String FAIL_MSG = "A repo exception was expected.";
  private static final Exception SQL_EXCEP = new SQLException();
  private static final String VALID_COLLECTION_KEY = "collection-1";
  private static final Integer VALID_VERSION = 0;

  @Mock
  private InputCollectionValidator inputCollectionValidator;

  @Mock
  private SqlService sqlService;

  @Mock
  private InputCollection inputCollection;

  @Mock
  private Bucket bucket;

  @Mock
  private List<RepoCollection> repoCollections;

  @Mock
  private RepoCollection expectedRepoCollection;

  @InjectMocks
  private CollectionRepoService collectionRepoService;


  @Before
  public void setUp() {
    collectionRepoService = new CollectionRepoService();
    initMocks(this);
  }

  @Test
  public void testListCollectionsHappyPath() throws RepoException, SQLException {
    doNothing().when(sqlService).getReadOnlyConnection();
    when(sqlService.getBucket(VALID_BUCKET)).thenReturn(bucket);

    when(sqlService.listCollectionsMetaData(VALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, VALID_TAG)).thenReturn(repoCollections);

    List<RepoCollection> response = collectionRepoService.listCollections(VALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, VALID_TAG);

    assertNotNull(response);
    assertEquals(response, repoCollections);

    verify(sqlService).getReadOnlyConnection();
    verify(sqlService).getBucket(VALID_BUCKET);
    verify(sqlService).listCollectionsMetaData(VALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, VALID_TAG);
  }

  @Test
  public void testListCollectionsInvalidBucket() throws RepoException, SQLException {
    doNothing().when(sqlService).getReadOnlyConnection();
    when(sqlService.getBucket(INVALID_BUCKET)).thenReturn(null);

    List<RepoCollection> response = null;
    try {
      response = collectionRepoService.listCollections(INVALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, VALID_TAG);
      fail(FAIL_MSG);
    } catch (RepoException re) {
      assertNull(response);
      assertEquals(re.getType(), RepoException.Type.BucketNotFound);
      verify(sqlService).getReadOnlyConnection();
      verify(sqlService).getBucket(INVALID_BUCKET);
    }
  }

  @Test
  public void testListCollectionsRepoServiceThrowsExc() throws RepoException, SQLException {
    doNothing().when(sqlService).getReadOnlyConnection();
    when(sqlService.getBucket(VALID_BUCKET)).thenReturn(bucket);

    when(sqlService.listCollectionsMetaData(VALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, VALID_TAG)).thenThrow(SQL_EXCEP);

    List<RepoCollection> response = null;

    try {
      response = collectionRepoService.listCollections(VALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, VALID_TAG);
      fail(FAIL_MSG);
    } catch (RepoException re) {
      assertNull(response);
      assertEquals(re.getCause(), SQL_EXCEP);
      verify(sqlService).getReadOnlyConnection();
      verify(sqlService).getBucket(VALID_BUCKET);
      verify(sqlService).listCollectionsMetaData(VALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, VALID_TAG);
    }
  }

  @Test
  public void testGetCollectionHappyPath() throws RepoException, SQLException {
    doNothing().when(sqlService).getReadOnlyConnection();

    RepoCollection expRepoCollection = new RepoCollection();
    when(sqlService.getCollection(VALID_BUCKET, VALID_COLLECTION_KEY, VALID_VERSION, VALID_TAG, null)).thenReturn(expRepoCollection);

    RepoCollection repoCollectionResp = collectionRepoService.getCollection(VALID_BUCKET, VALID_COLLECTION_KEY, new ElementFilter(VALID_VERSION, VALID_TAG, null));

    assertNotNull(repoCollectionResp);
    assertEquals(repoCollectionResp, expRepoCollection);

    verify(sqlService, times(1)).getReadOnlyConnection();
    verify(sqlService).getCollection(VALID_BUCKET, VALID_COLLECTION_KEY, VALID_VERSION, VALID_TAG, null);
  }

  @Test
  public void testGetCollectionVersionHappyPath() throws RepoException, SQLException {
    doNothing().when(sqlService).getReadOnlyConnection();

    List<RepoCollection> expRepoCollections = new ArrayList<>();
    RepoCollection coll1 = mock(RepoCollection.class);
    RepoCollection coll2 = mock(RepoCollection.class);
    expRepoCollections.add(coll1);
    expRepoCollections.add(coll2);
    when(sqlService.listCollectionVersions(VALID_BUCKET, VALID_COLLECTION_KEY)).thenReturn(expRepoCollections);

    List<RepoCollection> collectionsResp = collectionRepoService.getCollectionVersions(VALID_BUCKET, VALID_COLLECTION_KEY);

    assertNotNull(collectionsResp);
    assertEquals(2, collectionsResp.size());

    verify(sqlService).getReadOnlyConnection();
    verify(sqlService).listCollectionVersions(VALID_BUCKET, VALID_COLLECTION_KEY);
  }

  @Test
  public void testGetCollectionVersionNoKey() throws RepoException, SQLException {
    doNothing().when(sqlService).getReadOnlyConnection();

    List<RepoCollection> collectionsResp = null;
    try {
      collectionsResp = collectionRepoService.getCollectionVersions(VALID_BUCKET, "");
      fail("A repo exception was expected");
    } catch (RepoException e) {
      assertEquals(RepoException.Type.NoCollectionKeyEntered, e.getType());
      assertNull(collectionsResp);
      verify(sqlService).getReadOnlyConnection();
    }
  }

  @Test
  public void testGetCollectionVersionNoBucket() throws RepoException, SQLException {
    doNothing().when(sqlService).getReadOnlyConnection();

    List<RepoCollection> collectionsResp = null;
    try {
      collectionsResp = collectionRepoService.getCollectionVersions("", "");
      fail("A repo exception was expected");
    } catch (RepoException e) {
      assertEquals(RepoException.Type.NoBucketEntered, e.getType());
      assertNull(collectionsResp);
      verify(sqlService).getReadOnlyConnection();
    }
  }

}
