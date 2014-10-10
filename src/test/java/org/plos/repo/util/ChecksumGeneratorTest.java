package org.plos.repo.util;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.plos.repo.models.Collection;
import org.plos.repo.models.Object;
import org.plos.repo.service.RepoException;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * Checksum generator test
 */
public class ChecksumGeneratorTest {

  private static final Timestamp TIMESTAMP = new Timestamp(new Date().getTime());
  private static final String OBJECT_KEY1 = "object1";
  private static final String OBJECT_KEY2 = "object2";
  private static final String OBJ1_VERSION_CHECKSUM = "dnaskjndas15456dsadass";
  private static final String OBJ2_VERSION_CHECKSUM = "kandkasd3465dsabjdsbad";
  private static final String OBJ3_VERSION_CHECKSUM = "nkdlsadas15316diojda13";
  private static final String OBJ1_CHECKSUM = "1223123";
  private static final String TAG = "draft";
  private static final String CONTENT_TYPE = "text/plain";
  private static final String DOWNLOAD_NAME = "draft_object";
  private static final String CONTENT_TYPE1 = "image/jpg";
  private static final String COLLECTION_KEY = "collection";

  private ChecksumGenerator checksumGenerator;

  @Mock
  private Collection collection1;
  @Mock
  private Collection collection2;

  @Mock
  private Object object1;
  @Mock
  private Object object2;

  private List<String> objects1Checksum = Arrays.asList(new String[]{OBJ1_VERSION_CHECKSUM, OBJ2_VERSION_CHECKSUM});
  private List<String> objects2Checksum = Arrays.asList(new String[]{OBJ2_VERSION_CHECKSUM, OBJ1_VERSION_CHECKSUM});

  @Before
  public void setUp(){
    checksumGenerator = new ChecksumGenerator();
    object1 = new Object();
    object2 = new Object();
    initMocks(this);
  }

  @Test
  public void generateChecksumForSameCollection() throws RepoException {

    mockCollectionCalls(collection1, COLLECTION_KEY, TIMESTAMP, TAG);
    mockCollectionCalls(collection2, COLLECTION_KEY, TIMESTAMP, TAG);

    String checksumColl1 = checksumGenerator.generateVersionChecksum(collection1, objects1Checksum);
    String checksumColl2 = checksumGenerator.generateVersionChecksum(collection2, objects2Checksum);

    assertNotNull(checksumColl1);
    assertNotNull(checksumColl2);
    assertEquals(checksumColl1, checksumColl2);

    verifyCollectionCalls(collection1, 2);
    verifyCollectionCalls(collection2, 2);

  }


  @Test
  public void generateChecksumForDifObjsCollection() throws RepoException {

    mockCollectionCalls(collection1, COLLECTION_KEY, TIMESTAMP, TAG);
    mockCollectionCalls(collection2, COLLECTION_KEY, TIMESTAMP, null);

    String checksumColl1 = checksumGenerator.generateVersionChecksum(collection1, objects1Checksum);
    String checksumColl2 = checksumGenerator.generateVersionChecksum(collection2, objects1Checksum);

    assertNotNull(checksumColl1);
    assertNotNull(checksumColl2);
    assertNotEquals(checksumColl1, checksumColl2);

    verifyCollectionCalls(collection1, 2);
    verifyCollectionCalls(collection2, 1);

  }

  @Test
  public void generateChecksumForSameObject() throws RepoException {

    mockObjectCalls(object1, OBJECT_KEY1, TIMESTAMP, TAG, CONTENT_TYPE, DOWNLOAD_NAME, OBJ1_CHECKSUM);
    mockObjectCalls(object2, OBJECT_KEY1, TIMESTAMP, TAG, CONTENT_TYPE, DOWNLOAD_NAME, OBJ1_CHECKSUM);

    String checksumObj1 = checksumGenerator.generateVersionChecksum(object1);
    String checksumObj2 = checksumGenerator.generateVersionChecksum(object2);

    assertNotNull(checksumObj1);
    assertNotNull(checksumObj2);
    assertEquals(checksumObj1, checksumObj2);

  }

  @Test
  public void generateChecksumForDifObject() throws RepoException {

    mockObjectCalls(object1, OBJECT_KEY1, TIMESTAMP, TAG, CONTENT_TYPE, DOWNLOAD_NAME, OBJ1_CHECKSUM);
    mockObjectCalls(object2, OBJECT_KEY1, TIMESTAMP, TAG, CONTENT_TYPE1, null, OBJ1_CHECKSUM);

    String checksumObj1 = checksumGenerator.generateVersionChecksum(object1);
    String checksumObj2 = checksumGenerator.generateVersionChecksum(object2);

    assertNotNull(checksumObj1);
    assertNotNull(checksumObj2);
    assertNotEquals(checksumObj1, checksumObj2);

    verifyObjectCalls(object1, 2, 2 ,2);
    verifyObjectCalls(object2, 2, 2, 1);

  }
  
  private void mockObjectCalls(Object object, String key, Timestamp timestamp, String tag, String contentType, String downloadName, String checksum) {
    when(object.getKey()).thenReturn(key);
    when(object.getCreationDate()).thenReturn(timestamp);
    when(object.getTag()).thenReturn(tag);
    when(object.getContentType()).thenReturn(contentType);
    when(object.getDownloadName()).thenReturn(downloadName);
    when(object.getChecksum()).thenReturn(checksum);
  }

  private void verifyObjectCalls(Object object, int tagTimes, int contentTypeTimes, int downloadNameTimes) {
    verify(object).getKey();
    verify(object).getCreationDate();
    verify(object, times(tagTimes)).getTag();
    verify(object, times(contentTypeTimes)).getContentType();
    verify(object, times(downloadNameTimes)).getDownloadName();
    verify(object).getChecksum();
  }

  private void mockCollectionCalls(Collection collection, String collectionKey, Timestamp creationDateTime, String tag) {
    when(collection.getKey()).thenReturn(collectionKey);
    when(collection.getCreationDate()).thenReturn(creationDateTime);
    when(collection.getTag()).thenReturn(tag);
  }

  private void verifyCollectionCalls(Collection collection, int tagTimes){
    verify(collection).getKey();
    verify(collection).getCreationDate();
    verify(collection, times(tagTimes)).getTag();
  }

}
