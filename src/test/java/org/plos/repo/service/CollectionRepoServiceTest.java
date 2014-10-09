package org.plos.repo.service;


import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.plos.repo.models.*;
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
  private List<Collection> collections;

  @Mock
  private Collection expectedCollection;

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

    when(sqlService.listCollectionsMetaData(VALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, VALID_TAG)).thenReturn(collections);

    List<Collection> response = collectionRepoService.listCollections(VALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, VALID_TAG);

    assertNotNull(response);
    assertEquals(response, collections);

    verify(sqlService).getConnection();
    verify(sqlService).getBucket(VALID_BUCKET);
    verify(sqlService).listCollectionsMetaData(VALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, VALID_TAG);

  }

  @Test
  public void testListCollectionsInvalidBucket() throws RepoException, SQLException {

    doNothing().when(sqlService).getConnection();
    when(sqlService.getBucket(INVALID_BUCKET)).thenReturn(null);

    List<Collection> response = null;
    try{
      response = collectionRepoService.listCollections(INVALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, VALID_TAG);
      fail(FAIL_MSG);
    } catch(RepoException re){
      assertNull(response);
      assertEquals(re.getType(), RepoException.Type.BucketNotFound);
      verify(sqlService).getConnection();
      verify(sqlService).getBucket(INVALID_BUCKET);
    }

  }

  @Test
  public void testListCollectionsRepoServiceThrowsExc() throws RepoException, SQLException {

    doNothing().when(sqlService).getConnection();
    when(sqlService.getBucket(VALID_BUCKET)).thenReturn(bucket);

    when(sqlService.listCollectionsMetaData(VALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, VALID_TAG)).thenThrow(SQL_EXCEP);

    List<Collection> response = null;

    try{
      response = collectionRepoService.listCollections(VALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, VALID_TAG);
      fail(FAIL_MSG);
    } catch(RepoException re){
      assertNull(response);
      assertEquals(re.getCause(), SQL_EXCEP);
      verify(sqlService).getConnection();
      verify(sqlService).getBucket(VALID_BUCKET);
      verify(sqlService).listCollectionsMetaData(VALID_BUCKET, VALID_OFFSET, VALID_LIMIT, true, VALID_TAG);
    }

  }

  @Test
  public void testGetCollectionHappyPath() throws RepoException, SQLException {

    doNothing().when(sqlService).getConnection();

    Collection expCollection = new Collection();
    when(sqlService.getCollection(VALID_BUCKET, VALID_COLLECTION_KEY, VALID_VERSION, VALID_TAG, null)).thenReturn(expCollection);

    Collection collectionResp = collectionRepoService.getCollection(VALID_BUCKET, VALID_COLLECTION_KEY, new ElementFilter(VALID_VERSION, VALID_TAG, null));

    assertNotNull(collectionResp);
    assertEquals(collectionResp, expCollection);

    verify(sqlService, times(1)).getConnection();
    verify(sqlService).getCollection(VALID_BUCKET, VALID_COLLECTION_KEY, VALID_VERSION, VALID_TAG, null);

  }

  @Test
  public void testGetCollectionVersionHappyPath() throws RepoException, SQLException {

    doNothing().when(sqlService).getConnection();

    List<Collection> expCollections = new ArrayList<Collection>();
    Collection coll1 = mock(Collection.class);
    Collection coll2 = mock(Collection.class);
    expCollections.add(coll1);
    expCollections.add(coll2);
    when(sqlService.listCollectionVersions(VALID_BUCKET, VALID_COLLECTION_KEY)).thenReturn(expCollections);

    List<Collection> collectionsResp = collectionRepoService.getCollectionVersions(VALID_BUCKET, VALID_COLLECTION_KEY);

    assertNotNull(collectionsResp);
    assertEquals(2, collectionsResp.size());

    verify(sqlService).getConnection();
    verify(sqlService).listCollectionVersions(VALID_BUCKET, VALID_COLLECTION_KEY);

  }

  @Test
  public void testGetCollectionVersionNoKey() throws RepoException, SQLException {

    doNothing().when(sqlService).getConnection();

    List<Collection> collectionsResp = null;
    try{
      collectionsResp = collectionRepoService.getCollectionVersions(VALID_BUCKET, "");
      fail("A repo exception was expected");
    }catch(RepoException e){
      assertEquals(RepoException.Type.NoCollectionKeyEntered, e.getType());
      assertNull(collectionsResp);
      verify(sqlService).getConnection();
    }

  }

  @Test
  public void testGetCollectionVersionNoBucket() throws RepoException, SQLException {

    doNothing().when(sqlService).getConnection();

    List<Collection> collectionsResp = null;
    try{
      collectionsResp = collectionRepoService.getCollectionVersions("", "");
      fail("A repo exception was expected");
    }catch(RepoException e){
      assertEquals(RepoException.Type.NoBucketEntered, e.getType());
      assertNull(collectionsResp);
      verify(sqlService).getConnection();
    }

  }

}