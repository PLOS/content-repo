package org.plos.repo.util;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.plos.repo.models.*;
import org.plos.repo.models.Object;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * Checksum generator test
 */
public class VersionChecksumGeneratorTest {

  private static final Timestamp TIMESTAMP = new Timestamp(new Date().getTime());
  private static final Integer OBJ1_VERSION_CHECKSUM = 1234567;
  private static final Integer OBJ2_VERSION_CHECKSUM = 7654321;
  private static final Integer OBJ3_VERSION_CHECKSUM = 9999999;
  private static final String OBJ1_CHECKSUM = "1223123";
  private static final String TAG = "draft";
  private static final String CONTENT_TYPE = "text/plain";
  private static final String DOWNLOAD_NAME = "draft_object";
  private static final String CONTENT_TYPE1 = "image/jpg";

  private VersionChecksumGenerator versionChecksumGenerator;

  @Mock
  private Collection collection1;
  @Mock
  private Collection collection2;

  @Mock
  private Object object1;
  @Mock
  private Object object2;

  private List<Integer> objects1Checksum = Arrays.asList(new Integer[]{OBJ1_VERSION_CHECKSUM, OBJ2_VERSION_CHECKSUM});
  private List<Integer> objects2Checksum = Arrays.asList(new Integer[]{OBJ2_VERSION_CHECKSUM, OBJ1_VERSION_CHECKSUM});
  private List<Integer> objects3Checksum = Arrays.asList(new Integer[]{OBJ2_VERSION_CHECKSUM, OBJ3_VERSION_CHECKSUM});

  @Before
  public void setUp(){
    versionChecksumGenerator = new VersionChecksumGenerator();
    initMocks(this);
  }

  @Test
  public void generateChecksumForSameCollection(){

    mockCollectionCalls(collection1);
    mockCollectionCalls(collection2);

    Integer checksumColl1 = versionChecksumGenerator.generateVersionChecksum(collection1, objects1Checksum);
    Integer checksumColl2 = versionChecksumGenerator.generateVersionChecksum(collection2, objects2Checksum);

    assertNotNull(checksumColl1);
    assertNotNull(checksumColl2);
    assertEquals(checksumColl1, checksumColl2);

    verifyCollectionCalls(collection1);
    verifyCollectionCalls(collection2);

  }


  @Test
  public void generateChecksumForDifObjsCollection(){

    mockCollectionCalls(collection1);
    mockCollectionCalls(collection2);

    Integer checksumColl1 = versionChecksumGenerator.generateVersionChecksum(collection1, objects1Checksum);
    Integer checksumColl2 = versionChecksumGenerator.generateVersionChecksum(collection2, objects3Checksum);

    assertNotNull(checksumColl1);
    assertNotNull(checksumColl2);
    assertNotEquals(checksumColl1, checksumColl2);

    verifyCollectionCalls(collection1);
    verifyCollectionCalls(collection2);

  }

  @Test
  public void generateChecksumForSameObject(){

    objectSetup(object1, TIMESTAMP, TAG, CONTENT_TYPE, DOWNLOAD_NAME, OBJ1_CHECKSUM);
    objectSetup(object2, TIMESTAMP, TAG, CONTENT_TYPE, DOWNLOAD_NAME, OBJ1_CHECKSUM);

    Integer checksumObj1 = versionChecksumGenerator.generateVersionChecksum(object1);
    Integer checksumObj2 = versionChecksumGenerator.generateVersionChecksum(object2);

    assertNotNull(checksumObj1);
    assertNotNull(checksumObj2);
    assertEquals(checksumObj1, checksumObj2);

  }

  @Test
  public void generateChecksumForDifObject(){

    objectSetup(object1, TIMESTAMP, TAG, CONTENT_TYPE, DOWNLOAD_NAME, OBJ1_CHECKSUM);
    objectSetup(object2, TIMESTAMP, TAG, CONTENT_TYPE1, DOWNLOAD_NAME, OBJ1_CHECKSUM);

    Integer checksumObj1 = versionChecksumGenerator.generateVersionChecksum(object1);
    Integer checksumObj2 = versionChecksumGenerator.generateVersionChecksum(object2);

    assertNotNull(checksumObj1);
    assertNotNull(checksumObj2);
    assertNotEquals(checksumObj1, checksumObj2);

  }
  
  private void objectSetup(Object object, Timestamp timestamp, String tag, String contentType, String downloadName, String checksum) {
    object.timestamp = timestamp ;
    object.creationDate = timestamp;
    object.tag = tag;
    object.contentType = contentType;
    object.downloadName = downloadName;
    object.checksum = checksum;
  }

  private void mockCollectionCalls(Collection collection) {
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
