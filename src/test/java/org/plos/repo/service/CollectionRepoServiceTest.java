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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
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
  public void setUp(){

    collectionRepoService = new CollectionRepoService();
    initMocks(this);

  }

  @Test
  public void testListCollectionsHappyPath() throws RepoException, SQLException {

    doNothing().when(sqlService).getReadOnlyConnection();;
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
    try{
      response = collectionRepoService.listCollections(INVALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, VALID_TAG);
      fail(FAIL_MSG);
    } catch(RepoException re){
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

    try{
      response = collectionRepoService.listCollections(VALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, VALID_TAG);
      fail(FAIL_MSG);
    } catch(RepoException re){
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

    List<RepoCollection> expRepoCollections = new ArrayList<RepoCollection>();
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
    try{
      collectionsResp = collectionRepoService.getCollectionVersions(VALID_BUCKET, "");
      fail("A repo exception was expected");
    }catch(RepoException e){
      assertEquals(RepoException.Type.NoCollectionKeyEntered, e.getType());
      assertNull(collectionsResp);
      verify(sqlService).getReadOnlyConnection();
    }

  }

  @Test
  public void testGetCollectionVersionNoBucket() throws RepoException, SQLException {

    doNothing().when(sqlService).getReadOnlyConnection();

    List<RepoCollection> collectionsResp = null;
    try{
      collectionsResp = collectionRepoService.getCollectionVersions("", "");
      fail("A repo exception was expected");
    }catch(RepoException e){
      assertEquals(RepoException.Type.NoBucketEntered, e.getType());
      assertNull(collectionsResp);
      verify(sqlService).getReadOnlyConnection();
    }

  }

}
