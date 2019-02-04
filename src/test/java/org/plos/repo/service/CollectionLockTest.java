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

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.plos.repo.RepoBaseSpringTest;
import org.plos.repo.models.RepoCollection;
import org.plos.repo.models.RepoObject;
import org.plos.repo.models.input.ElementFilter;
import org.plos.repo.models.input.InputCollection;
import org.plos.repo.models.input.InputObject;
import org.plos.repo.models.input.InputRepoObject;

import javax.inject.Inject;
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CollectionLockTest extends RepoBaseSpringTest {

  private static final String BUCKET_NAME = "bucket";
  private static final String OBJECT_KEY = "object";
  private static final String COLLECTION_KEY = "collection";

  private final Timestamp CREATION_DATE_TIME = new Timestamp(new Date().getTime());

  private static final String OBJECT_DATA = "12345";

  private SqlService spySqlService;

  private CountDownLatch startGate;
  private CountDownLatch endGate;

  private List<InputObject> inputObjects;

  @Inject
  private CollectionRepoService collectionRepoService;

  /**
   * JUnit only captures assertion errors raised in the main thread, so we'll create an explicit error instance to
   * record assertion failures in in worker threads (only the first). Guard access with lock object.
   */
  private AssertionError assertionFailure;
  private final java.lang.Object lock = new java.lang.Object();


  // implement callback using interface and anonymous inner class
  public interface Callback {
    String getKeyname(int i);

    String getTag(int i);

    Timestamp getTimestamp();
  }

  @Before
  public void setup() throws Exception {

    repoService.createBucket(BUCKET_NAME, CREATION_DATE_TIME.toString());

    spySqlService = spy(this.sqlService);

    Field sqlServiceField = CollectionRepoService.class.getSuperclass().getDeclaredField("sqlService");
    sqlServiceField.setAccessible(true);
    sqlServiceField.set(collectionRepoService, spySqlService);

    inputObjects = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      String key = OBJECT_KEY + i;
      InputRepoObject inputRepoObject = new InputRepoObject();
      inputRepoObject.setKey(key);
      inputRepoObject.setBucketName(BUCKET_NAME);
      inputRepoObject.setUploadedInputStream(IOUtils.toInputStream(OBJECT_DATA));
      inputRepoObject.setTag("TAG" + i);
      inputRepoObject.setTimestamp(CREATION_DATE_TIME.toString());
      inputRepoObject.setCreationDateTime(CREATION_DATE_TIME.toString());
      RepoObject repoObject = repoService.createObject(RepoService.CreateMethod.NEW, inputRepoObject);
      InputObject inputObject = new InputObject(key, repoObject.getUuid().toString());
      inputObjects.add(inputObject);
    }

    this.startGate = new CountDownLatch(1);  // make all thread starts at the same time. Since all threads are going to be waiting on startGate, once all thread are created, we perform a startGate.countDown()
  }

  /*@Test*/
  // TODO: decide if these tests are needed or not
  public void testReaderAndWritersSameKeyAndSameData() throws Exception {
    final int INSERT_THREADS = 100;
    final int UPDATE_THREADS = 100;
    final int DELETE_THREADS = 0;
    final int READER_THREADS = 100;

    Callback callback = new Callback() {
      public String getKeyname(int i) {
        return COLLECTION_KEY;
      }

      public String getTag(int i) {
        return null;
      }

      @Override
      public Timestamp getTimestamp() {
        return CREATION_DATE_TIME;
      }
    };

    this.endGate = new CountDownLatch(INSERT_THREADS + DELETE_THREADS + READER_THREADS);
    execute(INSERT_THREADS, 0, DELETE_THREADS, READER_THREADS, inputObjects, callback);
    List<RepoCollection> repoCollections = collectionRepoService.listCollections(BUCKET_NAME, null, null, false, null);
    assertEquals(1, repoCollections.size()); // since all the collections where are trying to write are equals & they have

    this.startGate = new CountDownLatch(1);
    this.endGate = new CountDownLatch(UPDATE_THREADS + DELETE_THREADS + READER_THREADS);
    execute(0, UPDATE_THREADS, DELETE_THREADS, READER_THREADS, inputObjects, callback);
    repoCollections = collectionRepoService.listCollections(BUCKET_NAME, null, null, false, null);
    assertEquals(1, repoCollections.size());

    RepoCollection coll = repoCollections.get(0);

    assertEquals(COLLECTION_KEY, coll.getKey());
    assertEquals(Integer.valueOf(0), coll.getVersionNumber());

    verify(spySqlService, times(INSERT_THREADS + READER_THREADS * 2 + UPDATE_THREADS)).getCollection(anyString(), anyString()); // create new collection + list objects, when tag is null + update collection (when looking for exisiting ones)
    verify(spySqlService, times(inputObjects.size())).insertCollectionObjects(anyInt(), anyInt());
  }

  @Test
  public void createCollectionsAndNewVersionForEachCollTest() throws Exception {
    final int INSERT_THREADS = 100;
    final int UPDATE_THREADS = 100;
    final int DELETE_THREADS = 0;
    final int READER_THREADS = 100;

    Callback callback = new Callback() {
      public String getKeyname(int i) {
        return COLLECTION_KEY + i;
      }

      public String getTag(int i) {
        return null;
      }

      @Override
      public Timestamp getTimestamp() {
        return new Timestamp(new Date().getTime());
      }
    };

    this.endGate = new CountDownLatch(INSERT_THREADS + DELETE_THREADS + READER_THREADS);
    execute(INSERT_THREADS, 0, DELETE_THREADS, READER_THREADS, inputObjects, callback);
    List<RepoCollection> repoCollections = collectionRepoService.listCollections(BUCKET_NAME, null, null, false, null);
    assertEquals(INSERT_THREADS, repoCollections.size());

    Callback callbackUp = new Callback() {
      public String getKeyname(int i) {
        return COLLECTION_KEY + i;
      }

      public String getTag(int i) {
        return "TAG" + i;
      }

      @Override
      public Timestamp getTimestamp() {
        return new Timestamp(new Date().getTime());
      }
    };
    this.startGate = new CountDownLatch(1);
    this.endGate = new CountDownLatch(UPDATE_THREADS + DELETE_THREADS + READER_THREADS);
    execute(0, UPDATE_THREADS, DELETE_THREADS, READER_THREADS, inputObjects, callbackUp);

    repoCollections = collectionRepoService.listCollections(BUCKET_NAME, null, null, false, null);
    assertEquals(INSERT_THREADS + UPDATE_THREADS, repoCollections.size());

    verify(spySqlService, times(INSERT_THREADS + READER_THREADS + UPDATE_THREADS)).getCollection(anyString(), anyString()); // create new collection + list objects, when tag is null + update collection (when looking for exisiting ones)
    verify(spySqlService, times(READER_THREADS)).getCollection(anyString(), anyString(), anyInt(), anyString(), any(UUID.class)); // reading collections with tags
    verify(spySqlService, times(INSERT_THREADS + UPDATE_THREADS)).getCollectionNextAvailableVersion(anyString(), anyString()); // when creating and versioning a collection
    verify(spySqlService, times(INSERT_THREADS + UPDATE_THREADS)).insertCollection(any(RepoCollection.class)); // when creating and versioning a collection
    verify(spySqlService, times((INSERT_THREADS + UPDATE_THREADS) * inputObjects.size())).insertCollectionObjects(anyInt(), anyInt());
  }


  private void execute(final int insertThreads, final int updateThreads,
                       final int deleteThreads,
                       final int readerThreads,
                       final List<InputObject> objects,
                       final Callback cb)
      throws InterruptedException {
/*------------------------------------------------------------------

   INSERT

------------------------------------------------------------------*/


    for (int i = 0; i < insertThreads; i++) {
      final int j = i;
      final Thread t = new Thread() {
        public void run() {
          try {
            startGate.await();  // don't start until startGate is 0
            try {
              InputCollection inputColl = createInputCollection(cb, objects, j, RepoService.CreateMethod.NEW.toString());

              RepoCollection repoCollection = collectionRepoService.createCollection(RepoService.CreateMethod.NEW, inputColl);

              if (!repoCollection.getKey().equals(cb.getKeyname(j))) {
                synchronized (lock) {
                  if (assertionFailure == null) {
                    assertionFailure = new AssertionError(String.format(
                        "Expected:%s Actual:%s Reason:%s", cb.getKeyname(j), repoCollection.getKey(),
                        "insert failed"));
                  }
                }
              }
            } catch (RepoException e) {
              if (e.getType() == RepoException.Type.ServerError) {
                synchronized (lock) {
                  if (assertionFailure == null) {
                    assertionFailure = new AssertionError(String.format(
                        "Can not create new object:%s Reason:%s", cb.getKeyname(j),
                        e.getMessage()));
                  }
                }
              }
            } finally {
              endGate.countDown();
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };
      t.start();
    }

/*------------------------------------------------------------------

   UPDATE

------------------------------------------------------------------*/


    for (int i = 0; i < updateThreads; i++) {
      final int j = i;
      final Thread t = new Thread() {
        public void run() {
          try {
            startGate.await();  // don't start until startGate is 0
            try {
              InputCollection inputColl = createInputCollection(cb, objects, j, RepoService.CreateMethod.VERSION.toString());
              RepoCollection repoCollection = collectionRepoService.createCollection(RepoService.CreateMethod.VERSION, inputColl);

              if (!repoCollection.getKey().equals(cb.getKeyname(j))) {
                synchronized (lock) {
                  if (assertionFailure == null) {
                    assertionFailure = new AssertionError(String.format(
                        "Expected:%s Actual:%s Reason:%s", cb.getKeyname(j), repoCollection.getKey(),
                        "insert failed"));
                  }
                }
              }
            } catch (RepoException e) {
              if (e.getType() == RepoException.Type.ServerError) {
                synchronized (lock) {
                  if (assertionFailure == null) {
                    assertionFailure = new AssertionError(String.format(
                        "Can not version object:%s Reason:%s", cb.getKeyname(j),
                        e.getMessage()));
                  }
                }
              }
            } finally {
              endGate.countDown();
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };
      t.start();
    }

  /*------------------------------------------------------------------

  DELETE

------------------------------------------------------------------*/


    for (int i = 0; i < deleteThreads; i++) {
      final int j = i;
      final Thread t = new Thread() {
        public void run() {
          try {
            startGate.await();  // don't start until startGate is 0
            try {
              collectionRepoService.deleteCollection(BUCKET_NAME, cb.getKeyname(j), new ElementFilter(null, cb.getTag(j), null));
            } finally {
              endGate.countDown();
            }
          } catch (RepoException e) {
            if (e.getType() != RepoException.Type.ObjectNotFound) {
              synchronized (lock) {
                if (assertionFailure == null) {
                  assertionFailure = new AssertionError(String.format(
                      "Delete failed for:%s Reason:%s", cb.getKeyname(j), e.getMessage()));
                }
              }
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };
      t.start();
    }

/*------------------------------------------------------------------

  READER

------------------------------------------------------------------*/

    for (int i = 0; i < readerThreads; i++) {
      final int j = i;//(i % insertThreads);
      final Thread t = new Thread() {
        public void run() {
          try {
            startGate.await();  // don't start until startGate is 0
            try {
              RepoCollection repoCollection = collectionRepoService.getCollection(BUCKET_NAME, cb.getKeyname(j), new ElementFilter(null, cb.getTag(j), null));

              if (!repoCollection.getKey().equals(cb.getKeyname(j))) {
                synchronized (lock) {
                  if (assertionFailure == null) {
                    assertionFailure = new AssertionError(String.format(
                        "Expected:%s Actual:%s Reason:%s", cb.getKeyname(j), repoCollection.getKey(),
                        "read metadata failed"));
                  }
                }
              }
            } finally {
              endGate.countDown();
            }
          } catch (RepoException e) {
            if (e.getType() != RepoException.Type.CollectionNotFound) {
              synchronized (lock) {
                if (assertionFailure == null) {
                  assertionFailure = new AssertionError(String.format(
                      "Read failed for:%s Reason:%s", cb.getKeyname(j), e.getMessage()));
                }
              }
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };
      t.start();
    }

    startGate.countDown();
    //start all client threads

    endGate.await();
    //wait until all threads have finished (L=0)


    if (this.assertionFailure != null) {
      throw this.assertionFailure;
    }
  }

  private InputCollection createInputCollection(Callback cb, List<InputObject> objects, int threadNumber, String method) {
    InputCollection inputColl = new InputCollection();
    inputColl.setKey(cb.getKeyname(threadNumber));
    inputColl.setTimestamp(cb.getTimestamp().toString());
    inputColl.setBucketName(BUCKET_NAME);
    inputColl.setCreate(method);
    inputColl.setTag(cb.getTag(threadNumber));
    inputColl.setObjects(objects);
    inputColl.setCreationDateTime(cb.getTimestamp().toString());
    return inputColl;
  }

}
