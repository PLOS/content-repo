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

package org.plos.repo.util;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.plos.repo.models.RepoCollection;
import org.plos.repo.models.RepoObject;
import org.plos.repo.service.RepoException;

import java.security.MessageDigest;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
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
  private RepoObject repoObject1;
  @Mock
  private RepoObject repoObject2;

  private List<String> objects1Checksum = Arrays.asList(OBJ1_VERSION_CHECKSUM, OBJ2_VERSION_CHECKSUM);
  private List<String> objects2Checksum = Arrays.asList(OBJ2_VERSION_CHECKSUM, OBJ1_VERSION_CHECKSUM);

  @Before
  public void setUp() {
    checksumGenerator = new ChecksumGenerator();
    repoObject1 = new RepoObject();
    repoObject1.setId(0);
    repoObject1.setKey("keyObj1");
    repoObject2 = new RepoObject();
    repoObject2.setId(1);
    repoObject2.setKey("keyObj2");
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
    mockObjectCalls(repoObject1, OBJECT_KEY1, TIMESTAMP, TAG, CONTENT_TYPE, DOWNLOAD_NAME, OBJ1_CHECKSUM);
    mockObjectCalls(repoObject2, OBJECT_KEY1, TIMESTAMP, TAG, CONTENT_TYPE, DOWNLOAD_NAME, OBJ1_CHECKSUM);

    String checksumObj1 = checksumGenerator.generateVersionChecksum(repoObject1);
    String checksumObj2 = checksumGenerator.generateVersionChecksum(repoObject2);

    assertNotNull(checksumObj1);
    assertNotNull(checksumObj2);
    assertEquals(checksumObj1, checksumObj2);
  }

  @Test
  public void generateChecksumForDifObject() throws RepoException {
    mockObjectCalls(repoObject1, OBJECT_KEY1, TIMESTAMP, TAG, CONTENT_TYPE, DOWNLOAD_NAME, OBJ1_CHECKSUM);
    mockObjectCalls(repoObject2, OBJECT_KEY1, TIMESTAMP, TAG, CONTENT_TYPE1, null, OBJ1_CHECKSUM);

    String checksumObj1 = checksumGenerator.generateVersionChecksum(repoObject1);
    String checksumObj2 = checksumGenerator.generateVersionChecksum(repoObject2);

    assertNotNull(checksumObj1);
    assertNotNull(checksumObj2);
    assertNotEquals(checksumObj1, checksumObj2);

    verifyObjectCalls(repoObject1, 2, 2, 2);
    verifyObjectCalls(repoObject2, 2, 2, 1);
  }

  private void mockObjectCalls(RepoObject repoObject, String key, Timestamp timestamp, String tag, String contentType, String downloadName, String checksum) {
    when(repoObject.getKey()).thenReturn(key);
    when(repoObject.getCreationDate()).thenReturn(timestamp);
    when(repoObject.getTag()).thenReturn(tag);
    when(repoObject.getContentType()).thenReturn(contentType);
    when(repoObject.getDownloadName()).thenReturn(downloadName);
    when(repoObject.getChecksum()).thenReturn(checksum);
  }

  private void verifyObjectCalls(RepoObject repoObject, int tagTimes, int contentTypeTimes, int downloadNameTimes) {
    verify(repoObject).getKey();
    verify(repoObject).getCreationDate();
    verify(repoObject, times(tagTimes)).getTag();
    verify(repoObject, times(contentTypeTimes)).getContentType();
    verify(repoObject, times(downloadNameTimes)).getDownloadName();
    verify(repoObject).getChecksum();
  }

  private void mockCollectionCalls(RepoCollection repoCollection, String collectionKey, Timestamp creationDateTime, String tag) {
    when(repoCollection.getKey()).thenReturn(collectionKey);
    when(repoCollection.getCreationDate()).thenReturn(creationDateTime);
    when(repoCollection.getTag()).thenReturn(tag);
  }

  private void verifyCollectionCalls(RepoCollection repoCollection, int tagTimes) {
    verify(repoCollection).getKey();
    verify(repoCollection).getCreationDate();
    verify(repoCollection, times(tagTimes)).getTag();
  }

  @Test
  public void testEncode() throws RepoException {
    String expected = "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33";
    String input = "foo";
    ChecksumGenerator checksumGenerator = new ChecksumGenerator();
    MessageDigest digest = checksumGenerator.getDigestMessage();
    digest.update(input.getBytes());
    assertEquals(expected, checksumGenerator.checksumToString(digest.digest()));
  }
}
