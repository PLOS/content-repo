package org.plos.repo.service;


import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.Collection;
import org.plos.repo.models.InputCollection;
import org.plos.repo.models.InputCollectionValidator;

import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class CollectionRepoServiceTest {

  private static final String VALID_BUCKET = "bucket-1";
  private static final Integer VALID_OFFSET = 1;
  private static final Integer VALID_LIMIT = 100;
  private static final String VALID_TAG = "tag";

  @Mock
  private InputCollectionValidator inputCollectionValidator;

  @Mock
  private SqlService sqlService;

  @Mock
  private InputCollection inputCollection;

  @Mock
  private Bucket bucket;

  @Mock
  List<Collection> collections;

  @InjectMocks
  private CollectionRepoService collectionRepoService;

  @Before
  public void setUp(){

    collectionRepoService = new CollectionRepoService();
    initMocks(this);

  }

  @Test
  public void testListCollectionsHappyPath() throws RepoException, SQLException {

    doNothing().when(sqlService).getConnection();
    when(sqlService.getBucket(VALID_BUCKET)).thenReturn(bucket);

    when(sqlService.listCollections(VALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, VALID_TAG)).thenReturn(collections);

    List<Collection> response = collectionRepoService.listCollections(VALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, VALID_TAG);

    assertNotNull(response);
    assertEquals(response, collections);

    verify(sqlService).getConnection();
    verify(sqlService).getBucket(VALID_BUCKET);
    verify(sqlService).listCollections(VALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, VALID_TAG);



  }



}
