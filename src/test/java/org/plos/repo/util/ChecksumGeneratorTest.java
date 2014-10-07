/*
package org.plos.repo.util;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.plos.repo.models.*;
import org.plos.repo.models.Object;
import org.plos.repo.service.RepoException;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

*/
/**
 * Checksum generator test
 *//*

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

  private Object object1;
  private Object object2;

  private List<String> objects1Checksum = Arrays.asList(new String[]{OBJ1_VERSION_CHECKSUM, OBJ2_VERSION_CHECKSUM});
  private List<String> objects2Checksum = Arrays.asList(new String[]{OBJ2_VERSION_CHECKSUM, OBJ1_VERSION_CHECKSUM});
  private List<String> objects3Checksum = Arrays.asList(new String[]{OBJ2_VERSION_CHECKSUM, OBJ3_VERSION_CHECKSUM});

  @Before
  public void setUp(){
    checksumGenerator = new ChecksumGenerator();
    object1 = new Object();
    object2 = new Object();
    initMocks(this);
  }

  @Test
  public void generateChecksumForSameCollection() throws RepoException {

    mockCollectionCalls(collection1);
    mockCollectionCalls(collection2);

    String checksumColl1 = checksumGenerator.generateVersionChecksum(collection1, objects1Checksum);
    String checksumColl2 = checksumGenerator.generateVersionChecksum(collection2, objects2Checksum);

    assertNotNull(checksumColl1);
    assertNotNull(checksumColl2);
    assertEquals(checksumColl1, checksumColl2);

    verifyCollectionCalls(collection1);
    verifyCollectionCalls(collection2);

  }


  @Test
  public void generateChecksumForDifObjsCollection() throws RepoException {

    mockCollectionCalls(collection1);
    mockCollectionCalls(collection2);

    String checksumColl1 = checksumGenerator.generateVersionChecksum(collection1, objects1Checksum);
    String checksumColl2 = checksumGenerator.generateVersionChecksum(collection2, objects3Checksum);

    assertNotNull(checksumColl1);
    assertNotNull(checksumColl2);
    assertNotEquals(checksumColl1, checksumColl2);

    verifyCollectionCalls(collection1);
    verifyCollectionCalls(collection2);

  }

  @Test
  public void generateChecksumForSameObject() throws RepoException {

    objectSetup(object1, OBJECT_KEY1, TIMESTAMP, TAG, CONTENT_TYPE, DOWNLOAD_NAME, OBJ1_CHECKSUM);
    objectSetup(object2, OBJECT_KEY1, TIMESTAMP, TAG, CONTENT_TYPE, DOWNLOAD_NAME, OBJ1_CHECKSUM);

    String checksumObj1 = checksumGenerator.generateVersionChecksum(object1);
    String checksumObj2 = checksumGenerator.generateVersionChecksum(object2);

    assertNotNull(checksumObj1);
    assertNotNull(checksumObj2);
    assertEquals(checksumObj1, checksumObj2);

  }

  @Test
  public void generateChecksumForDifObject() throws RepoException {

    objectSetup(object1, OBJECT_KEY1, TIMESTAMP, TAG, CONTENT_TYPE, DOWNLOAD_NAME, OBJ1_CHECKSUM);
    objectSetup(object2, OBJECT_KEY2, TIMESTAMP, TAG, CONTENT_TYPE1, DOWNLOAD_NAME, OBJ1_CHECKSUM);

    String checksumObj1 = checksumGenerator.generateVersionChecksum(object1);
    String checksumObj2 = checksumGenerator.generateVersionChecksum(object2);

    assertNotNull(checksumObj1);
    assertNotNull(checksumObj2);
    assertNotEquals(checksumObj1, checksumObj2);

  }
  
  private void objectSetup(Object object, String key, Timestamp timestamp, String tag, String contentType, String downloadName, String checksum) {
    object.setKey(key);
    object.setTimestamp(timestamp);
    object.setCreationDate(timestamp);
    object.setTag(tag);
    object.setContentType(contentType);
    object.setDownloadName(downloadName);
    object.setChecksum(checksum);
  }

  private void mockCollectionCalls(Collection collection) {
    when(collection.getKey()).thenReturn(COLLECTION_KEY);
    when(collection.getTimestamp()).thenReturn(TIMESTAMP);
    when(collection.getCreationDate()).thenReturn(TIMESTAMP);
    when(collection.getStatus()).thenReturn(Status.USED);
  }

  private void verifyCollectionCalls(Collection collection){
    verify(collection).getTimestamp();
    verify(collection).getCreationDate();
    verify(collection).getStatus();
  }

}
*/
