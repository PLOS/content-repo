package org.plos.repo.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.Test;
import org.plos.repo.RepoBaseTest;
import org.plos.repo.models.Bucket;
import org.plos.repo.rest.BucketController;

public class BucketLockTest extends RepoBaseTest {

  private static final String BUCKET_NAME = "bucket";

  private BucketController bucketCtl;

  private ObjectStore      spyObjectStore;
  private SqlService       spySqlService;

  private CountDownLatch   startGate;
  private CountDownLatch   endGate;

  @Before public void setup() throws Exception {

    clearData();

    bucketCtl      = new BucketController();
    spyObjectStore = spy(this.objectStore);
    spySqlService  = spy(this.sqlService);

    // use reflection to inject object store and sql services ("spied on" versions)
    // into bucket controller.
    
    Field objStoreField = BucketController.class.getDeclaredField("objectStore");
    objStoreField.setAccessible(true);
    objStoreField.set(bucketCtl, spyObjectStore);

    Field sqlServiceField = BucketController.class.getDeclaredField("sqlService");
    sqlServiceField.setAccessible(true);
    sqlServiceField.set(bucketCtl, spySqlService);

    this.startGate = new CountDownLatch(1);
  }

  @Test public void testManyInserts() throws Exception {

    final int INSERT_THREADS = 10;
    final int DELETE_THREADS = 0;
    final int READER_THREADS = 0;

    this.endGate = new CountDownLatch(INSERT_THREADS + DELETE_THREADS + READER_THREADS);

    execute(INSERT_THREADS, DELETE_THREADS, READER_THREADS);

    verify(spySqlService, times(INSERT_THREADS)).getBucketId(anyString());
    verify(spyObjectStore, times(1)).createBucket(any(Bucket.class));
    verify(spySqlService, times(1)).insertBucket(any(Bucket.class));
    verify(spyObjectStore, never()).deleteBucket(any(Bucket.class));

    List<Bucket> buckets = this.sqlService.listBuckets();
    assertEquals(1, buckets.size());
  }

  @Test public void testManyDeletesForExistingBucket() throws Exception {

      bucketCtl.create(BUCKET_NAME);

      final int INSERT_THREADS = 0;
      final int DELETE_THREADS = 5;
      final int READER_THREADS = 0;

      this.endGate = new CountDownLatch(INSERT_THREADS + DELETE_THREADS + READER_THREADS);

      execute(INSERT_THREADS, DELETE_THREADS, READER_THREADS);

      // get bucket id is called for each delete plus initial create call
      verify(spySqlService, times(DELETE_THREADS+1)).getBucketId(anyString());
      verify(spyObjectStore, times(1)).deleteBucket(any(Bucket.class));
      verify(spySqlService, times(1)).deleteBucket(anyString());
  }

  @Test public void testInsertsAndDeletes() throws Exception {

    final int INSERT_THREADS = 10;
    final int DELETE_THREADS = 10;
    final int READER_THREADS = 0;

    this.endGate = new CountDownLatch(INSERT_THREADS + DELETE_THREADS + READER_THREADS);

    execute(INSERT_THREADS, DELETE_THREADS, READER_THREADS);

    // do final insert in case the last thread operation was a delete. 
    bucketCtl.create(BUCKET_NAME);

    List<Bucket> buckets = this.sqlService.listBuckets();
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
              bucketCtl.create(BUCKET_NAME);
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
      final Thread t = new Thread() {
        public void run() {
          try {
            startGate.await();  /* don't start until startGate is 0 */
            try {
              bucketCtl.delete(BUCKET_NAME);
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
    /*  READER                                                            */
    /* ------------------------------------------------------------------ */
    // TODO
    
    startGate.countDown(); /* start all client threads                    */
    endGate.await();       /* wait until all threads have finished (L=0)  */
    }
}
