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

package org.plos.repo.util;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.plos.repo.models.RepoCollection;
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
  private static final String OBJ1_VERSION_CHECKSUM = "dnaskjndas15456dsadass";
  private static final String OBJ2_VERSION_CHECKSUM = "kandkasd3465dsabjdsbad";
  private static final String OBJ1_CHECKSUM = "1223123";
  private static final String TAG = "draft";
  private static final String CONTENT_TYPE = "text/plain";
  private static final String DOWNLOAD_NAME = "draft_object";
  private static final String CONTENT_TYPE1 = "image/jpg";
  private static final String COLLECTION_KEY = "collection";

  private ChecksumGenerator checksumGenerator;

  @Mock
  private RepoCollection repoCollection1;
  @Mock
  private RepoCollection repoCollection2;

  @Mock
  private Object object1;
  @Mock
  private Object object2;

  private List<String> objects1Checksum = Arrays.asList(new String[]{OBJ1_VERSION_CHECKSUM, OBJ2_VERSION_CHECKSUM});
  private List<String> objects2Checksum = Arrays.asList(new String[]{OBJ2_VERSION_CHECKSUM, OBJ1_VERSION_CHECKSUM});

  @Before
  public void setUp(){
    checksumGenerator = new ChecksumGenerator();
    object1 = new Object(0,"keyObj1",null,null,null,null,null,null,null,null,null,null,null,null);
    object2 = new Object(0,"keyObj2",null,null,null,null,null,null,null,null,null,null,null,null);
    initMocks(this);
  }

  @Test
  public void generateChecksumForSameCollection() throws RepoException {

    mockCollectionCalls(repoCollection1, COLLECTION_KEY, TIMESTAMP, TAG);
    mockCollectionCalls(repoCollection2, COLLECTION_KEY, TIMESTAMP, TAG);

    String checksumColl1 = checksumGenerator.generateVersionChecksum(repoCollection1, objects1Checksum);
    String checksumColl2 = checksumGenerator.generateVersionChecksum(repoCollection2, objects2Checksum);

    assertNotNull(checksumColl1);
    assertNotNull(checksumColl2);
    assertEquals(checksumColl1, checksumColl2);

    verifyCollectionCalls(repoCollection1, 2);
    verifyCollectionCalls(repoCollection2, 2);

  }


  @Test
  public void generateChecksumForDifObjsCollection() throws RepoException {

    mockCollectionCalls(repoCollection1, COLLECTION_KEY, TIMESTAMP, TAG);
    mockCollectionCalls(repoCollection2, COLLECTION_KEY, TIMESTAMP, null);

    String checksumColl1 = checksumGenerator.generateVersionChecksum(repoCollection1, objects1Checksum);
    String checksumColl2 = checksumGenerator.generateVersionChecksum(repoCollection2, objects1Checksum);

    assertNotNull(checksumColl1);
    assertNotNull(checksumColl2);
    assertNotEquals(checksumColl1, checksumColl2);

    verifyCollectionCalls(repoCollection1, 2);
    verifyCollectionCalls(repoCollection2, 1);

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

  private void mockCollectionCalls(RepoCollection repoCollection, String collectionKey, Timestamp creationDateTime, String tag) {
    when(repoCollection.getKey()).thenReturn(collectionKey);
    when(repoCollection.getCreationDate()).thenReturn(creationDateTime);
    when(repoCollection.getTag()).thenReturn(tag);
  }

  private void verifyCollectionCalls(RepoCollection repoCollection, int tagTimes){
    verify(repoCollection).getKey();
    verify(repoCollection).getCreationDate();
    verify(repoCollection, times(tagTimes)).getTag();
  }

}
