package org.plos.repo.service;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.plos.repo.RepoBaseSpringTest;
import org.plos.repo.models.Object;
import org.plos.repo.models.input.ElementFilter;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

//TODO - HACK. Remove this when integrate with Service layer code

public class ObjectLockTest extends RepoBaseSpringTest {

  private static final String BUCKET_NAME = "bucket";
  private static final String BASE_KEY_NAME = "key";
  private final Timestamp CREATION_DATE_TIME = new Timestamp(new Date().getTime());

  private static final String OBJECT_DATA = "12345";

  private ObjectStore spyObjectStore;
  private SqlService spySqlService;

  private CountDownLatch startGate;
  private CountDownLatch endGate;

    /**
    * JUnit only captures assertion errors raised in the main thread, so we'll
    * create an explicit error instance to record assertion failures in 
    * in worker threads (only the first). Guard access with lock object.
    */

  private AssertionError assertionFailure;
  private final java.lang.Object lock = new java.lang.Object();


  // implement callback using interface and anonymous inner class
  public interface Callback {
    String getKeyname(int i);

    String getTag(int i);
  }

  @Before
  public void setup() throws Exception {

    clearData(objectStore, sqlService);

    repoService.createBucket(BUCKET_NAME, CREATION_DATE_TIME.toString());

    spyObjectStore = spy(this.objectStore);
    spySqlService = spy(this.sqlService);

    Field osObjStoreField = RepoService.class.getDeclaredField("objectStore");
    osObjStoreField.setAccessible(true);
    osObjStoreField.set(repoService, spyObjectStore);

    Field osSqlServiceField = BaseRepoService.class.getDeclaredField("sqlService");
    osSqlServiceField.setAccessible(true);
    osSqlServiceField.set(repoService, spySqlService);

    this.startGate = new CountDownLatch(1);  // make all thread starts at the same time. Since all threads are going to be waiting on startGate, once all thread are created, we perform a startGate.countDown()
  }

  @Test
  public void testReaderAndWritersSameKeyAndSameData() throws Exception {

    final int INSERT_THREADS = 25;
    final int UPDATE_THREADS = 100;
    final int DELETE_THREADS = 0;
    final int READER_THREADS = 100;

    Callback callback = new Callback() {
      public String getKeyname(int i) {
        return BASE_KEY_NAME;
      }

      public String getTag(int i) {
        return null;
      }
    };

    this.endGate = new CountDownLatch(INSERT_THREADS + DELETE_THREADS + READER_THREADS);
    execute(INSERT_THREADS, 0, DELETE_THREADS, READER_THREADS, callback);

    this.startGate = new CountDownLatch(1);
    this.endGate = new CountDownLatch(UPDATE_THREADS + DELETE_THREADS + READER_THREADS);
    execute(0, UPDATE_THREADS, DELETE_THREADS, READER_THREADS, callback);

    List<org.plos.repo.models.Object> objects = repoService.listObjects(BUCKET_NAME, null, null, false, null);
    // since when version an object we don't create a new version if nothing change from last version, only one object with BASE_KEY_NAME is going to be created
    assertEquals(1, objects.size());

    org.plos.repo.models.Object obj = objects.get(0);

    assertEquals(BASE_KEY_NAME, obj.getKey());
    assertEquals(Integer.valueOf(0), obj.getVersionNumber());
    assertTrue(this.objectStore.objectExists(obj));

    List<Object> versions = repoService.getObjectVersions(obj.getBucketName(), obj.getKey());

    assertEquals(1, versions.size());

    // create new objects + list objects + update objects + list objects ----> all operations calls getObject underneath
    verify(spySqlService, times(READER_THREADS*3 + INSERT_THREADS)).getObject(anyString(), anyString());

  }

  @Test
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

      assertEquals(BASE_KEY_NAME, obj.getKey());
      assertTrue(this.objectStore.objectExists(obj));

      List<Object> versions = repoService.getObjectVersions(obj.getBucketName(), obj.getKey());

      assertEquals(1 + UPDATE_THREADS, versions.size());
    }

    verify(spySqlService, times(READER_THREADS*2)).getObject(anyString(), anyString(), anyInt(), anyString(), anyString());
  }


  @Test

  // TODO : rewrite test to include the new changes
  public void testReaderAndWritersSameKeyWithDeletions() throws Exception {

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

      assertEquals(BASE_KEY_NAME, obj.getKey());
      assertTrue(this.objectStore.objectExists(obj));

    }

    verify(spySqlService, times(INSERT_THREADS + UPDATE_THREADS + READER_THREADS)).getObject(anyString(), anyString()); // insert object + list objects + update objets

    verify(spySqlService, times(DELETE_THREADS + READER_THREADS*2)).getObject(anyString(), anyString(), anyInt(), anyString(), anyString()); // reading objects (3 times) + deleting objects
  }

@Test

  // TODO : rewrite test to include the new changes
  public void testReaderAndWritersDifferentKeys() throws Exception {

    final int INSERT_THREADS = 100;
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
      assertEquals(Integer.valueOf(0), obj.getVersionNumber());
      assertTrue(this.objectStore.objectExists(obj));
    }

    // when deleting an object, we first verify that the object is not contain in an active collection. For that end, we look for the existing object using sqlService getObject
    verify(spySqlService, times(READER_THREADS + DELETE_THREADS)).getObject(anyString(), anyString(), anyInt(), anyString(), anyString());
  }

  private void execute(final int insertThreads, final int updateThreads,
                       final int deleteThreads,
                       final int readerThreads, final Callback cb)
      throws InterruptedException {

/* ------------------------------------------------------------------

   INSERT

 ------------------------------------------------------------------ */


    for (int i = 0; i < insertThreads; i++) {
      final int j = i;
      final Thread t = new Thread() {
        public void run() {
          try {
            startGate.await();  // don't start until startGate is 0
            try {
              Timestamp creationDateTime = new Timestamp(new Date().getTime());
              Object object = repoService.createObject(RepoService.CreateMethod.NEW, cb.getKeyname(j), BUCKET_NAME, null, null, creationDateTime, IOUtils.toInputStream(OBJECT_DATA), creationDateTime, cb.getTag(j));

              if (!object.getKey().equals(cb.getKeyname(j))) {
                synchronized (lock) {
                  if (assertionFailure == null) {
                    assertionFailure = new AssertionError(String.format(
                        "Expected:%s Actual:%s Reason:%s", cb.getKeyname(j), object.getKey(),
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

 /* ------------------------------------------------------------------

   UPDATE

 ------------------------------------------------------------------ */


    for (int i = 0; i < updateThreads; i++) {
      final int j = i;
      final Thread t = new Thread() {
        public void run() {
          try {
            startGate.await();  // don't start until startGate is 0
            try {
              Timestamp creationDateTime = new Timestamp(new Date().getTime());
              Object versionedObject = repoService.createObject(RepoService.CreateMethod.VERSION, cb.getKeyname(j), BUCKET_NAME, null, null, creationDateTime, IOUtils.toInputStream(OBJECT_DATA), creationDateTime, cb.getTag(j));

              if (!versionedObject.getKey().equals(cb.getKeyname(j))) {
                synchronized (lock) {
                  if (assertionFailure == null) {
                    assertionFailure = new AssertionError(String.format(
                        "Expected:%s Actual:%s Reason:%s", cb.getKeyname(j), versionedObject.getKey(),
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

/*  ------------------------------------------------------------------

  DELETE

 ------------------------------------------------------------------ */


    for (int i = 0; i < deleteThreads; i++) {
      final int j = i;
      final Thread t = new Thread() {
        public void run() {
          try {
            startGate.await();  // don't start until startGate is 0 
            try {
              repoService.deleteObject(BUCKET_NAME, cb.getKeyname(j), new ElementFilter(null, cb.getTag(j), null));
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

 /* ------------------------------------------------------------------

  READER

 ------------------------------------------------------------------ */


    for (int i = 0; i < readerThreads; i++) {
      final int j = i;//(i % insertThreads);
      final Thread t = new Thread() {
        public void run() {
          try {
            startGate.await();  // don't start until startGate is 0 
            try {

              Object object = repoService.getObject(BUCKET_NAME, cb.getKeyname(j), new ElementFilter(null, cb.getTag(j), null));

              String outputData = IOUtils.toString(repoService.getObjectInputStream(object));

              if (!object.getKey().equals(cb.getKeyname(j))) {
                synchronized (lock) {
                  if (assertionFailure == null) {
                    assertionFailure = new AssertionError(String.format(
                        "Expected:%s Actual:%s Reason:%s", cb.getKeyname(j), object.getKey(),
                        "read metadata failed"));
                  }
                }
              }

              if (!outputData.equals(OBJECT_DATA)) {
                synchronized (lock) {
                  if (assertionFailure == null) {
                    assertionFailure = new AssertionError(String.format(
                        "Expected:%s Actual:%s Reason:%s", OBJECT_DATA, outputData,
                        "data read mismatch"));
                  }
                }
              }

            } finally {
              endGate.countDown();
            }
          } catch (RepoException e) {

            if (e.getType() != RepoException.Type.ObjectNotFound) {

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

class ObjectComparator implements Comparator<org.plos.repo.models.Object> {
  public int compare(org.plos.repo.models.Object o1, org.plos.repo.models.Object o2) {

    if (o1.getBucketName().compareTo(o2.getBucketName()) < 0) {
      return -1;
    } else if (o1.getBucketName().compareTo(o2.getBucketName()) > 0) {
      return 1;
    }
    // o1.bucketName.equals(o2.bucketName)

    if (o1.getKey().compareTo(o2.getKey()) < 0) {
      return -1;
    } else if (o1.getKey().compareTo(o2.getKey()) > 0) {
      return 1;
    }
    // o1.key.equals(o2.key)

    if (o1.getVersionNumber().compareTo(o2.getVersionNumber()) < 0) {
      return -1;
    } else if (o1.getVersionNumber().compareTo(o2.getVersionNumber()) > 0) {
      return 1;
    }
    // o1.versionNumber.equals(o2.versionNumber)

    return 0;
  }
}
