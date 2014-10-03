package org.plos.repo.service;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.plos.repo.RepoBaseSpringTest;
import org.plos.repo.models.*;
import org.plos.repo.models.Object;

import javax.inject.Inject;
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

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
  protected CollectionRepoService collectionRepoService;

/**
   * JUnit only captures assertion errors raised in the main thread, so we'll
   * create an explicit error instance to record assertion failures in
   * in worker threads (only the first). Guard access with lock object.*/
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

    clearData(objectStore, sqlService);

    repoService.createBucket(BUCKET_NAME, CREATION_DATE_TIME.toString());

    spySqlService = spy(this.sqlService);

    Field sqlServiceField = CollectionRepoService.class.getSuperclass().getDeclaredField("sqlService");
    sqlServiceField.setAccessible(true);
    sqlServiceField.set(collectionRepoService, spySqlService);

    inputObjects = new ArrayList<InputObject>();
    for (int i=0; i < 1000 ; i++ ){
      String key = OBJECT_KEY+i;
      Object object = repoService.createObject(RepoService.CreateMethod.NEW, key, BUCKET_NAME, null, null, CREATION_DATE_TIME, IOUtils.toInputStream(OBJECT_DATA), CREATION_DATE_TIME, "TAG"+i);
      InputObject inputObject = new InputObject(key, object.versionChecksum);
      inputObjects.add(inputObject);
    }

    this.startGate = new CountDownLatch(1);  // make all thread starts at the same time. Since all threads are going to be waiting on startGate, once all thread are created, we perform a startGate.countDown()
  }

  @Test
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
      public Timestamp getTimestamp() { return CREATION_DATE_TIME;  }
    };

    this.endGate = new CountDownLatch(INSERT_THREADS + DELETE_THREADS + READER_THREADS);
    execute(INSERT_THREADS, 0, DELETE_THREADS, READER_THREADS, inputObjects, callback);
    List<org.plos.repo.models.Collection> collections = collectionRepoService.listCollections(BUCKET_NAME, null, null, false, null);
    assertEquals(1, collections.size()); // since all the collections where are trying to write are equals & they have

    this.startGate = new CountDownLatch(1);
    this.endGate = new CountDownLatch(UPDATE_THREADS + DELETE_THREADS + READER_THREADS);
    execute(0, UPDATE_THREADS, DELETE_THREADS, READER_THREADS, inputObjects, callback);
    collections = collectionRepoService.listCollections(BUCKET_NAME, null, null, false, null);
    assertEquals(1, collections.size());

    org.plos.repo.models.Collection coll = collections.get(0);

    assertEquals(COLLECTION_KEY, coll.getKey());
    assertEquals(Integer.valueOf(0), coll.getVersionNumber());

    verify(spySqlService, times(INSERT_THREADS + READER_THREADS*2 + UPDATE_THREADS)).getCollection(anyString(), anyString()); // create new collection + list objects, when tag is null + update collection (when looking for exisiting ones)
    verify(spySqlService, times(inputObjects.size())).insertCollectionObjects(anyInt(), anyString(), anyString(), anyString());

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
      public Timestamp getTimestamp() { return new Timestamp(new Date().getTime()); }
    };

    this.endGate = new CountDownLatch(INSERT_THREADS + DELETE_THREADS + READER_THREADS);
    execute(INSERT_THREADS, 0, DELETE_THREADS, READER_THREADS, inputObjects, callback);
    List<org.plos.repo.models.Collection> collections = collectionRepoService.listCollections(BUCKET_NAME, null, null, false, null);
    assertEquals(INSERT_THREADS, collections.size());

    Callback callbackUp = new Callback() {
      public String getKeyname(int i) {
        return COLLECTION_KEY + i;
      }

      public String getTag(int i) {
        return "TAG" + i;
      }

      @Override
      public Timestamp getTimestamp() { return new Timestamp(new Date().getTime()); }
    };
    this.startGate = new CountDownLatch(1);
    this.endGate = new CountDownLatch(UPDATE_THREADS + DELETE_THREADS + READER_THREADS);
    execute(0, UPDATE_THREADS, DELETE_THREADS, READER_THREADS, inputObjects, callbackUp);

    collections = collectionRepoService.listCollections(BUCKET_NAME, null, null, false, null);
    assertEquals(INSERT_THREADS + UPDATE_THREADS, collections.size());

    verify(spySqlService, times(INSERT_THREADS + READER_THREADS + UPDATE_THREADS)).getCollection(anyString(), anyString()); // create new collection + list objects, when tag is null + update collection (when looking for exisiting ones)
    verify(spySqlService, times(READER_THREADS)).getCollection(anyString(), anyString(), anyInt(), anyString(), anyString()); // reading collections with tags
    verify(spySqlService, times(INSERT_THREADS + UPDATE_THREADS)).getCollectionNextAvailableVersion(anyString(), anyString()); // when creating and versioning a collection
    verify(spySqlService, times(INSERT_THREADS + UPDATE_THREADS)).insertCollection(any(Collection.class)); // when creating and versioning a collection
    verify(spySqlService, times((INSERT_THREADS + UPDATE_THREADS)*inputObjects.size())).insertCollectionObjects(anyInt(), anyString(), anyString(), anyString());

  }

 /* @Test
  public void testReaderAndWritersSameKeyDifferentData() throws Exception {

    final int INSERT_THREADS = 25;
    final int UPDATE_THREADS = 100;
    final int DELETE_THREADS = 0;
    final int READER_THREADS = 100;

    Callback callback = new Callback() {
      public String getKeyname(int i) {
        return BASE_KEY_NAME;
      }

      public String getTag(int i) {
        return "TAG" + i;
      }

    };

    this.endGate = new CountDownLatch(INSERT_THREADS + DELETE_THREADS + READER_THREADS);
    execute(INSERT_THREADS, 0, DELETE_THREADS, READER_THREADS, callback);
    List<org.plos.repo.models.Object> objects = repoService.listObjects(BUCKET_NAME, null, null, false, null);
    // since when version an object we don't create a new version if nothing change from last version, only one object with BASE_KEY_NAME is going to be created
    assertEquals(1, objects.size());

    this.startGate = new CountDownLatch(1);
    this.endGate = new CountDownLatch(UPDATE_THREADS + DELETE_THREADS + READER_THREADS);
    execute(0, UPDATE_THREADS, DELETE_THREADS, READER_THREADS, callback);

    objects = repoService.listObjects(BUCKET_NAME, null, null, false, null);
    // since when version an object we don't create a new version if nothing change from last version, only one object with BASE_KEY_NAME is going to be created
    assertEquals(1 + UPDATE_THREADS, objects.size());

    for (int j = 0; j < objects.size(); j++) {
      org.plos.repo.models.Object obj = objects.get(j);

      assertEquals(BASE_KEY_NAME, obj.key);
      assertTrue(this.objectStore.objectExists(obj));

      List<Object> versions = repoService.getObjectVersions(obj);

      assertEquals(1 + UPDATE_THREADS, versions.size());
    }

    verify(spySqlService, times(READER_THREADS*2)).getObject(anyString(), anyString(), anyInt(), anyString(), anyString());
  }
*/

/*  @Test*/

  // TODO : rewrite test to include the new changes
 /* public void testReaderAndWritersSameKeyWithDeletions() throws Exception {

    final int INSERT_THREADS = 25;
    final int UPDATE_THREADS = 100;
    final int DELETE_THREADS = 10;
    final int READER_THREADS = 125;

    Callback callback = new Callback() {
      public String getKeyname(int i) { return BASE_KEY_NAME; }

      public String getTag(int i) { return null; }

    };

    this.endGate = new CountDownLatch(INSERT_THREADS + READER_THREADS);
    execute(INSERT_THREADS, 0, 0, READER_THREADS, callback);

    List<org.plos.repo.models.Object> objects = repoService.listObjects(BUCKET_NAME, null, null, false, null);
    assertEquals(1, objects.size());

    Callback callbackUp = new Callback() {
      public String getKeyname(int i) { return BASE_KEY_NAME; }

      public String getTag(int i) { return "TAG" + i; }

    };

    this.startGate = new CountDownLatch(1);
    this.endGate = new CountDownLatch(UPDATE_THREADS  + READER_THREADS);
    execute(0, UPDATE_THREADS, 0, READER_THREADS, callbackUp);

    objects = repoService.listObjects(BUCKET_NAME, null, null, false, null);
    assertEquals(1 + UPDATE_THREADS, objects.size());

    this.startGate = new CountDownLatch(1);
    this.endGate = new CountDownLatch(DELETE_THREADS + READER_THREADS);
    execute(0, 0, DELETE_THREADS, READER_THREADS, callbackUp);

    objects = repoService.listObjects(BUCKET_NAME, null, null, false, null);
    assertEquals(1 + UPDATE_THREADS - DELETE_THREADS, objects.size());

    Collections.sort(objects, new ObjectComparator());

    for (int j = 0; j < objects.size(); j++) {
      org.plos.repo.models.Object obj = objects.get(j);

      assertEquals(BASE_KEY_NAME, obj.key);
      assertTrue(this.objectStore.objectExists(obj));

    }

    verify(spySqlService, times(INSERT_THREADS + UPDATE_THREADS + READER_THREADS)).getObject(anyString(), anyString()); // insert object + list objects + update objets

    verify(spySqlService, times(DELETE_THREADS + READER_THREADS*2)).getObject(anyString(), anyString(), anyInt(), anyString(), anyString()); // reading objects (3 times) + deleting objects
  }*/

/*  @Test*/

  // TODO : rewrite test to include the new changes
 /* public void testReaderAndWritersDifferentKeys() throws Exception {

    final int INSERT_THREADS = 25;
    final int UPDATE_THREADS = 0;
    final int DELETE_THREADS = 20;
    final int READER_THREADS = 100;

    this.endGate = new CountDownLatch(INSERT_THREADS + UPDATE_THREADS + DELETE_THREADS + READER_THREADS);

    Callback callback = new Callback() {
      public String getKeyname(int i) {
        return BASE_KEY_NAME + String.format("%03d", i);
      }

      public String getTag(int i) { return "TAG" + i; }

    };

    execute(INSERT_THREADS, UPDATE_THREADS, DELETE_THREADS, READER_THREADS, callback);

    List<org.plos.repo.models.Object> objects = repoService.listObjects(BUCKET_NAME, null, null, false, null);

    assertTrue(objects.size() >= INSERT_THREADS - DELETE_THREADS);

    Collections.sort(objects, new ObjectComparator());

    for (int j = 0; j < objects.size(); j++) {
      org.plos.repo.models.Object obj = objects.get(j);
      assertEquals(Integer.valueOf(0), obj.versionNumber);
      assertTrue(this.objectStore.objectExists(obj));
    }

    // when deleting an object, we first verify that the object is not contain in an active collection. For that end, we look for the existing object using sqlService getObject
    verify(spySqlService, times(READER_THREADS + DELETE_THREADS)).getObject(anyString(), anyString(), anyInt(), anyString(), anyString());
  }*/

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
              InputCollection inputColl = new InputCollection(cb.getKeyname(j), cb.getTimestamp().toString(), BUCKET_NAME, RepoService.CreateMethod.NEW.toString(), cb.getTag(j),  objects, cb.getTimestamp().toString());
              Collection collection = collectionRepoService.createCollection2(RepoService.CreateMethod.NEW, inputColl);

              if (!collection.getKey().equals(cb.getKeyname(j))) {
                synchronized (lock) {
                  if (assertionFailure == null) {
                    assertionFailure = new AssertionError(String.format(
                        "Expected:%s Actual:%s Reason:%s", cb.getKeyname(j), collection.getKey(),
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
              InputCollection inputColl = new InputCollection(cb.getKeyname(j), cb.getTimestamp().toString(), BUCKET_NAME, RepoService.CreateMethod.VERSION.toString(), cb.getTag(j),  objects, cb.getTimestamp().toString());
              Collection collection = collectionRepoService.createCollection2(RepoService.CreateMethod.VERSION, inputColl);

              if (!collection.getKey().equals(cb.getKeyname(j))) {
                synchronized (lock) {
                  if (assertionFailure == null) {
                    assertionFailure = new AssertionError(String.format(
                        "Expected:%s Actual:%s Reason:%s", cb.getKeyname(j), collection.getKey(),
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

              Collection collection = collectionRepoService.getCollection(BUCKET_NAME, cb.getKeyname(j), new ElementFilter(null, cb.getTag(j), null));

              if (!collection.getKey().equals(cb.getKeyname(j))) {
                synchronized (lock) {
                  if (assertionFailure == null) {
                    assertionFailure = new AssertionError(String.format(
                        "Expected:%s Actual:%s Reason:%s", cb.getKeyname(j), collection.getKey(),
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

}
