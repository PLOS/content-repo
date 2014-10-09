package org.plos.repo.service;

import org.junit.Before;
import org.junit.Test;
import org.plos.repo.RepoBaseSpringTest;
import org.plos.repo.models.Bucket;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;


public class BucketLockTest extends RepoBaseSpringTest {

  private static final String BUCKET_NAME = "bucket";
  private static final Timestamp CREATION_DATE_TIMESTAMP = new Timestamp(new Date().getTime());
  private static final String CREATION_DATE_TIME_STRING = CREATION_DATE_TIMESTAMP.toString();

  private ObjectStore spyObjectStore;
  private SqlService spySqlService;

  private CountDownLatch startGate;
  private CountDownLatch endGate;

  @Before
  public void setup() throws Exception {

    clearData(objectStore, sqlService);

    spyObjectStore = spy(this.objectStore);
    spySqlService = spy(this.sqlService);

    // use reflection to inject object store and sql services ("spied on" versions)
    // into bucket controller.

    Field objStoreField = RepoService.class.getDeclaredField("objectStore");
    objStoreField.setAccessible(true);
    objStoreField.set(repoService, spyObjectStore);

    Field sqlServiceField = BaseRepoService.class.getDeclaredField("sqlService");
    sqlServiceField.setAccessible(true);
    sqlServiceField.set(repoService, spySqlService);

    this.startGate = new CountDownLatch(1);
  }

  @Test
  public void testManyInserts() throws Exception {

    final int INSERT_THREADS = 10;
    final int DELETE_THREADS = 0;
    final int READER_THREADS = 0;

    this.endGate = new CountDownLatch(INSERT_THREADS + DELETE_THREADS + READER_THREADS);

    execute(INSERT_THREADS, DELETE_THREADS, READER_THREADS);

    // +1 for getBucket at success return time
    verify(spySqlService, times(INSERT_THREADS + 1)).getBucket(anyString());
    verify(spyObjectStore, times(1)).createBucket(any(Bucket.class));
    verify(spySqlService, times(1)).insertBucket(any(Bucket.class), any(Timestamp.class));
    verify(spyObjectStore, never()).deleteBucket(any(Bucket.class));

    assertEquals(1, repoService.listBuckets().size());
  }

  @Test
  public void testManyDeletesForExistingBucket() throws Exception {

    repoService.createBucket(BUCKET_NAME, CREATION_DATE_TIME_STRING);

    final int INSERT_THREADS = 0;
    final int DELETE_THREADS = 5;
    final int READER_THREADS = 0;

    this.endGate = new CountDownLatch(INSERT_THREADS + DELETE_THREADS + READER_THREADS);

    execute(INSERT_THREADS, DELETE_THREADS, READER_THREADS);

    // +2 for createBucket
    verify(spySqlService, times(DELETE_THREADS + 2)).getBucket(anyString());
    verify(spyObjectStore, times(1)).deleteBucket(any(Bucket.class));
    verify(spySqlService, times(1)).deleteBucket(anyString());
  }

  @Test
  public void testInsertsAndDeletes() throws Exception {

    final int INSERT_THREADS = 10;
    final int DELETE_THREADS = 10;
    final int READER_THREADS = 0;

    this.endGate = new CountDownLatch(INSERT_THREADS + DELETE_THREADS + READER_THREADS);

    execute(INSERT_THREADS, DELETE_THREADS, READER_THREADS);

    // do final insert in case the last thread operation was a delete.
    try {
      repoService.createBucket(BUCKET_NAME, CREATION_DATE_TIME_STRING);
    } catch (RepoException e) {
      // toss the exception since we are only creating if it does not exist
    }

    List<Bucket> buckets = repoService.listBuckets();
    assertEquals(1, buckets.size());
  }

  private void execute(final int insertThreads, final int deleteThreads, final int readerThreads)
      throws Exception {

    /* ------------------------------------------------------------------ */
    /*  INSERT                                                            */
    /* ------------------------------------------------------------------ */

    for (int i = 0; i < insertThreads; i++) {
      final Thread t = new Thread() {
        public void run() {
          try {
            startGate.await();  /* don't start until startGate is 0 */
            try {
              repoService.createBucket(BUCKET_NAME, CREATION_DATE_TIME_STRING);
            } finally {
              endGate.countDown();
            }
          } catch (RepoException e) {
            // absorb this exception since it is often expected
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
      final Thread t = new Thread() {
        public void run() {
          try {
            startGate.await();  /* don't start until startGate is 0 */
            try {
              repoService.deleteBucket(BUCKET_NAME);
            } finally {
              endGate.countDown();
            }
          } catch (RepoException e) {
            // absorb this exception since it is often expected
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
    // TODO

    startGate.countDown(); /* start all client threads                    */
    endGate.await();       /* wait until all threads have finished (L=0)  */
  }
}
