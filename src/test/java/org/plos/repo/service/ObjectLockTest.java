package org.plos.repo.service;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.plos.repo.RepoBaseSpringTest;
import org.plos.repo.models.Object;

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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

//TODO - HACK. Remove this when integrate with Service layer code

public class ObjectLockTest extends RepoBaseSpringTest {

  private static final String BUCKET_NAME = "bucket";
  private static final String BASE_KEY_NAME = "key";

  private static final String OBJECT_DATA = "12345";

  private ObjectStore spyObjectStore;
  private SqlService spySqlService;

  private CountDownLatch startGate;
  private CountDownLatch endGate;

  /*
    * JUnit only captures assertion errors raised in the main thread, so we'll 
    * create an explicit error instance to record assertion failures in 
    * in worker threads (only the first). Guard access with lock object.
    */
  private AssertionError assertionFailure;
  private final java.lang.Object lock = new java.lang.Object();

  @Before
  public void setup() throws Exception {

    clearData(objectStore, sqlService);

    repoService.createBucket(BUCKET_NAME);

    spyObjectStore = spy(this.objectStore);
    spySqlService = spy(this.sqlService);

    Field osObjStoreField = RepoService.class.getDeclaredField("objectStore");
    osObjStoreField.setAccessible(true);
    osObjStoreField.set(repoService, spyObjectStore);

    Field osSqlServiceField = RepoService.class.getDeclaredField("sqlService");
    osSqlServiceField.setAccessible(true);
    osSqlServiceField.set(repoService, spySqlService);

    this.startGate = new CountDownLatch(1);
  }

  // implement callback using interface and anonymous inner class
  public interface Callback {
    String getKeyname(int i);

    Integer getVersion(int i);
  }


  @Test
  public void testReaderAndWritersSameKey() throws Exception {

    final int INSERT_THREADS = 25;
    final int UPDATE_THREADS = 100;
    final int DELETE_THREADS = 0;
    final int READER_THREADS = 100;

    Callback callback = new Callback() {
      public String getKeyname(int i) {
        return BASE_KEY_NAME;
      }

      public Integer getVersion(int i) {
        return i;
      }
    };

    this.endGate = new CountDownLatch(INSERT_THREADS + DELETE_THREADS + READER_THREADS);
    execute(INSERT_THREADS, 0, DELETE_THREADS, READER_THREADS, callback);

    this.startGate = new CountDownLatch(1);
    this.endGate = new CountDownLatch(UPDATE_THREADS + DELETE_THREADS + READER_THREADS);
    execute(0, UPDATE_THREADS, DELETE_THREADS, READER_THREADS, callback);


    List<org.plos.repo.models.Object> objects = repoService.listObjects(BUCKET_NAME, null, null, false);
    assertEquals(1 + UPDATE_THREADS, objects.size());

    Collections.sort(objects, new ObjectComparator());

    for (int j = 0; j < objects.size(); j++) {
      org.plos.repo.models.Object obj = objects.get(j);

      assertEquals(BASE_KEY_NAME, obj.key);
      assertEquals(Integer.valueOf(j), obj.versionNumber);
      assertTrue(this.objectStore.objectExists(obj));

      List<Object> versions = repoService.getObjectVersions(obj);

      assertEquals(UPDATE_THREADS + 1, versions.size());
    }

    verify(spySqlService, times(READER_THREADS*2)).getObject(anyString(), anyString(), anyInt());
  }


  @Test
  public void testReaderAndWritersSameKeyWithDeletions() throws Exception {

    final int INSERT_THREADS = 25;
    final int UPDATE_THREADS = 100;
    final int DELETE_THREADS = 10;
    final int READER_THREADS = 125;

    Callback callback = new Callback() {
      public String getKeyname(int i) {
        return BASE_KEY_NAME;
      }

      public Integer getVersion(int i) {
        return i;
      }
    };

    this.endGate = new CountDownLatch(INSERT_THREADS + READER_THREADS);
    execute(INSERT_THREADS, 0, 0, READER_THREADS, callback);

    this.startGate = new CountDownLatch(1);
    this.endGate = new CountDownLatch(UPDATE_THREADS  + READER_THREADS);
    execute(0, UPDATE_THREADS, 0, READER_THREADS, callback);

    this.startGate = new CountDownLatch(1);
    this.endGate = new CountDownLatch(DELETE_THREADS + READER_THREADS);
    execute(0, 0, DELETE_THREADS, READER_THREADS, callback);

    List<org.plos.repo.models.Object> objects = repoService.listObjects(BUCKET_NAME, null, null, false);
    assertEquals(1 + UPDATE_THREADS - DELETE_THREADS, objects.size());

    Collections.sort(objects, new ObjectComparator());

    for (int j = 0; j < objects.size(); j++) {
      org.plos.repo.models.Object obj = objects.get(j);

      assertEquals(BASE_KEY_NAME, obj.key);
      assertTrue(this.objectStore.objectExists(obj));

    }

    verify(spySqlService, times(READER_THREADS*3)).getObject(anyString(), anyString(), anyInt());
  }

  @Test
  public void testReaderAndWritersDifferentKeys() throws Exception {

    final int INSERT_THREADS = 25;
    final int UPDATE_THREADS = 0;
    final int DELETE_THREADS = 20;
    final int READER_THREADS = 100;

    this.endGate = new CountDownLatch(INSERT_THREADS + UPDATE_THREADS + DELETE_THREADS + READER_THREADS);

    Callback callback = new Callback() {
      public String getKeyname(int i) {
        return BASE_KEY_NAME + String.format("%03d", i);
      }

      public Integer getVersion(int i) {
        return 0;
      }
    };

    execute(INSERT_THREADS, UPDATE_THREADS, DELETE_THREADS, READER_THREADS, callback);

    List<org.plos.repo.models.Object> objects = repoService.listObjects(BUCKET_NAME, null, null, false);

    assertTrue(objects.size() >= INSERT_THREADS - DELETE_THREADS);

    Collections.sort(objects, new ObjectComparator());

    for (int j = 0; j < objects.size(); j++) {
      org.plos.repo.models.Object obj = objects.get(j);
      assertEquals(Integer.valueOf(0), obj.versionNumber);
      assertTrue(this.objectStore.objectExists(obj));
    }

    verify(spySqlService, times(READER_THREADS)).getObject(anyString(), anyString(), anyInt());
  }

  private void execute(final int insertThreads, final int updateThreads,
                       final int deleteThreads,
                       final int readerThreads, final Callback cb)
      throws InterruptedException {

    /* ------------------------------------------------------------------ */
    /*  INSERT                                                            */
    /* ------------------------------------------------------------------ */

    for (int i = 0; i < insertThreads; i++) {
      final int j = i;
      final Thread t = new Thread() {
        public void run() {
          try {
            startGate.await();  // don't start until startGate is 0
            try {

              Object object = repoService.createObject(RepoService.CreateMethod.NEW, cb.getKeyname(j), BUCKET_NAME, null, null, new Timestamp(new Date().getTime()), IOUtils.toInputStream(OBJECT_DATA));

              if (!object.key.equals(cb.getKeyname(j))) {
                synchronized (lock) {
                  if (assertionFailure == null) {
                    assertionFailure = new AssertionError(String.format(
                        "Expected:%s Actual:%s Reason:%s", cb.getKeyname(j), object.key,
                        "insert failed"));
                  }
                }
              }
            } catch (RepoException e) {

              if (e.getType() != RepoException.Type.ClientError) {
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

    /* ------------------------------------------------------------------ */
    /*  UPDATE                                                            */
    /* ------------------------------------------------------------------ */

    for (int i = 0; i < updateThreads; i++) {
      final int j = i;
      final Thread t = new Thread() {
        public void run() {
          try {
            startGate.await();  // don't start until startGate is 0
            try {

              Object versionedObject = repoService.createObject(RepoService.CreateMethod.VERSION, cb.getKeyname(j), BUCKET_NAME, null, null, new Timestamp(new Date().getTime()), IOUtils.toInputStream(OBJECT_DATA));

              if (!versionedObject.key.equals(cb.getKeyname(j))) {
                synchronized (lock) {
                  if (assertionFailure == null) {
                    assertionFailure = new AssertionError(String.format(
                        "Expected:%s Actual:%s Reason:%s", cb.getKeyname(j), versionedObject.key,
                        "insert failed"));
                  }
                }
              }

            } catch (RepoException e) {

              if (e.getType() != RepoException.Type.ClientError) {
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

    /* ------------------------------------------------------------------ */
    /*  DELETE                                                            */
    /* ------------------------------------------------------------------ */

    for (int i = 0; i < deleteThreads; i++) {
      final int j = i;
      final Thread t = new Thread() {
        public void run() {
          try {
            startGate.await();  // don't start until startGate is 0 
            try {
              repoService.deleteObject(BUCKET_NAME, cb.getKeyname(j), cb.getVersion(j));
            } finally {
              endGate.countDown();
            }
          } catch (RepoException e) {

            if (e.getType() != RepoException.Type.ItemNotFound) {

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

    /* ------------------------------------------------------------------ */
    /*  READER                                                            */
    /* ------------------------------------------------------------------ */

    for (int i = 0; i < readerThreads; i++) {
      final int j = i;//(i % insertThreads);
      final Thread t = new Thread() {
        public void run() {
          try {
            startGate.await();  // don't start until startGate is 0 
            try {

              Object object = repoService.getObject(BUCKET_NAME, cb.getKeyname(j), cb.getVersion(j));

              String outputData = IOUtils.toString(repoService.getObjectInputStream(object));

              if (!object.key.equals(cb.getKeyname(j))) {
                synchronized (lock) {
                  if (assertionFailure == null) {
                    assertionFailure = new AssertionError(String.format(
                        "Expected:%s Actual:%s Reason:%s", cb.getKeyname(j), object.key,
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

            if (e.getType() != RepoException.Type.ItemNotFound) {

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

    startGate.countDown(); /* start all client threads                    */
    endGate.await();       /* wait until all threads have finished (L=0)  */

    if (this.assertionFailure != null) {
      throw this.assertionFailure;
    }
  }

}

class ObjectComparator implements Comparator<org.plos.repo.models.Object> {
  public int compare(org.plos.repo.models.Object o1, org.plos.repo.models.Object o2) {

    if (o1.bucketName.compareTo(o2.bucketName) < 0) {
      return -1;
    } else if (o1.bucketName.compareTo(o2.bucketName) > 0) {
      return 1;
    }
    // o1.bucketName.equals(o2.bucketName)

    if (o1.key.compareTo(o2.key) < 0) {
      return -1;
    } else if (o1.key.compareTo(o2.key) > 0) {
      return 1;
    }
    // o1.key.equals(o2.key)

    if (o1.versionNumber.compareTo(o2.versionNumber) < 0) {
      return -1;
    } else if (o1.versionNumber.compareTo(o2.versionNumber) > 0) {
      return 1;
    }
    // o1.versionNumber.equals(o2.versionNumber)

    return 0;
  }
}
