package org.plos.repo.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;
import org.plos.repo.RepoBaseTest;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.Object;
import org.plos.repo.models.Object.Status;
import org.plos.repo.rest.ObjectController;

//TODO - HACK. Remove this when integrate with Service layer code
import javax.ws.rs.core.Response;

public class ObjectLockTest extends RepoBaseTest {

    private static final String BUCKET_NAME   = "bucket";
    private static final String BASE_KEY_NAME = "key";

    private ObjectController objectCtl;
    private RepoInfoService  repoInfoService;

    private ObjectStore      spyObjectStore;
    private SqlService       spySqlService;
    private RepoInfoService  spyRepoInfoService;

    private CountDownLatch   startGate;
    private CountDownLatch   endGate;

    @Before public void setup() throws Exception {

        clearData();

        Bucket bucket = new Bucket(null, BUCKET_NAME);
        if (!this.objectStore.createBucket(bucket)) { throw new RuntimeException("setup failed"); } 
        if (!this.sqlService.insertBucket(bucket))  { throw new RuntimeException("setup failed"); } 

        objectCtl      = new ObjectController();
        spyObjectStore = spy(this.objectStore);
        spySqlService  = spy(this.sqlService);

        repoInfoService    = new RepoInfoService();
        spyRepoInfoService = spy(repoInfoService);

        // use reflection to inject object store and sql services ("spied on" versions)
        // into object controller and repro info service.
       
        Field osObjStoreField = ObjectController.class.getDeclaredField("objectStore");
        osObjStoreField.setAccessible(true);
        osObjStoreField.set(objectCtl, spyObjectStore);

        Field osSqlServiceField = ObjectController.class.getDeclaredField("sqlService");
        osSqlServiceField.setAccessible(true);
        osSqlServiceField.set(objectCtl, spySqlService);

        Field rsObjStoreField = RepoInfoService.class.getDeclaredField("objectStore");
        rsObjStoreField.setAccessible(true);
        rsObjStoreField.set(repoInfoService, spyObjectStore);

        Field rsSqlServiceField = RepoInfoService.class.getDeclaredField("sqlService");
        rsSqlServiceField.setAccessible(true);
        rsSqlServiceField.set(repoInfoService, spySqlService);

        // inject repo info service into object controller

        Field repoInfoServiceField = ObjectController.class.getDeclaredField("repoInfoService");
        repoInfoServiceField.setAccessible(true);
        repoInfoServiceField.set(objectCtl, spyRepoInfoService);

        this.startGate = new CountDownLatch(1);
    }

    // implement callback using interface and anonymous inner class
    public interface Callback {
        String  getKeyname(int i);
        Integer getVersion(int i);
    }

    @Test public void testReaderAndWritersSameKey() throws Exception {

        final int INSERT_THREADS = 25;
        final int DELETE_THREADS = 20;
        final int READER_THREADS = 125;

        this.endGate = new CountDownLatch(INSERT_THREADS + DELETE_THREADS + READER_THREADS);

        Callback callback = new Callback() {
            public String  getKeyname(int i) { return BASE_KEY_NAME; } 
            public Integer getVersion(int i) { return i;             } 
        };

        execute(INSERT_THREADS, DELETE_THREADS, READER_THREADS, callback);

        List<org.plos.repo.models.Object> objects =  this.sqlService.listAllObject();
        assertEquals(INSERT_THREADS, objects.size());

        Collections.sort(objects, new ObjectComparator());

        int objDeleteCount = 0;

        for (int j = 0; j < INSERT_THREADS; j++) {
            org.plos.repo.models.Object obj = objects.get(j);
            if (obj.status == Object.Status.DELETED) { objDeleteCount++; }

            assertEquals(BASE_KEY_NAME, obj.key);
            assertEquals(Integer.valueOf(j), obj.versionNumber);
            assertTrue( this.objectStore.objectExists(obj) );
        }

        //TODO - these assertions could be a bit stronger if imposed more
        //       restrictions on threads such as briefly pausing before 
        //       trying to delete or lookup an object.

        assertTrue( objDeleteCount > 0 );
        assertTrue( getReadCount(this.repoInfoService) > 0 );

        assertEquals(INSERT_THREADS, getWriteCount(this.repoInfoService));

        verify(spySqlService, times(READER_THREADS)).getObject(anyString(), anyString(), anyInt());
    }

    @Test public void testReaderAndWritersDifferentKeys() throws Exception {

        final int INSERT_THREADS = 25;
        final int DELETE_THREADS = 20;
        final int READER_THREADS = 100;

        this.endGate = new CountDownLatch(INSERT_THREADS + DELETE_THREADS + READER_THREADS);

        Callback callback = new Callback() {
            public String  getKeyname(int i) { return BASE_KEY_NAME + String.format("%03d",i); } 
            public Integer getVersion(int i) { return 0;                 } 
        };

        execute(INSERT_THREADS, DELETE_THREADS, READER_THREADS, callback);

        List<org.plos.repo.models.Object> objects =  this.sqlService.listAllObject();
        assertEquals(INSERT_THREADS, objects.size());

        Collections.sort(objects, new ObjectComparator());

        int objDeleteCount = 0;

        for (int j = 0; j < INSERT_THREADS; j++) {
            org.plos.repo.models.Object obj = objects.get(j);
            if (obj.status == Object.Status.DELETED) { objDeleteCount++; }

            assertEquals(BASE_KEY_NAME + String.format("%03d",j), obj.key);
            assertEquals(Integer.valueOf(0), obj.versionNumber);
            assertTrue( this.objectStore.objectExists(obj) );
        }

        //TODO - these assertions could be a bit stronger if imposed more
        //       restrictions on threads such as briefly pausing before 
        //       trying to delete or lookup an object.

        assertTrue( objDeleteCount > 0 );
        assertTrue( getReadCount(this.repoInfoService) > 0 );

        assertEquals(INSERT_THREADS, getWriteCount(this.repoInfoService));

        verify(spySqlService, times(READER_THREADS)).getObject(anyString(), anyString(), anyInt());
    }

    private void execute(final int insertThreads, final int deleteThreads, 
                         final int readerThreads, final Callback cb) 
        throws Exception {

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
                            Response response = objectCtl.createOrUpdate(cb.getKeyname(j), BUCKET_NAME,
                                null, null, "auto", null, 
                                new ByteArrayInputStream("12345".getBytes("UTF-8")));

                            System.out.println(String.format(">>> key:%s idx:%d response:%s", 
                                cb.getKeyname(j), j, response));

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
                            Response response = objectCtl.delete(BUCKET_NAME, cb.getKeyname(j), cb.getVersion(j));

                            System.out.println(String.format(">>> key:%s version:%d idx:%d response:%s", 
                                cb.getKeyname(j), cb.getVersion(j), j, response));

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

        for (int i = 0; i < readerThreads; i++) {
            final int j = (i % insertThreads);
            final Thread t = new Thread() {
                public void run() {
                    try {
                        startGate.await();  // don't start until startGate is 0 
                        try {
                            Response response = objectCtl.read(BUCKET_NAME, cb.getKeyname(j), cb.getVersion(j), false, null);

                            System.out.println(String.format(">>> key:%s version:%d idx:%d response:%s", 
                                cb.getKeyname(j), cb.getVersion(j), j, response));

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

        startGate.countDown(); /* start all client threads                    */
        endGate.await();       /* wait until all threads have finished (L=0)  */
    }

    private long getReadCount(RepoInfoService repoInfoService) throws NoSuchFieldException, IllegalAccessException {
        Field readCountField = RepoInfoService.class.getDeclaredField("readCount");
        readCountField.setAccessible(true);
        return (long) ((AtomicLong)readCountField.get(repoInfoService)).longValue();
    }

    private long getWriteCount(RepoInfoService repoInfoService) throws NoSuchFieldException, IllegalAccessException {
        Field writeCountField = RepoInfoService.class.getDeclaredField("writeCount");
        writeCountField.setAccessible(true);
        return (long) ((AtomicLong)writeCountField.get(repoInfoService)).longValue();
    }
}

class ObjectComparator implements Comparator<org.plos.repo.models.Object> {
    public int compare(org.plos.repo.models.Object o1, org.plos.repo.models.Object o2) {

        if      (o1.bucketName.compareTo(o2.bucketName) < 0) { return -1; } 
        else if (o1.bucketName.compareTo(o2.bucketName) > 0) { return 1;  }
        // o1.bucketName.equals(o2.bucketName)

        if      (o1.key.compareTo(o2.key) < 0) { return -1; } 
        else if (o1.key.compareTo(o2.key) > 0) { return 1;  }
        // o1.key.equals(o2.key)

        if      (o1.versionNumber.compareTo(o2.versionNumber) < 0) { return -1; } 
        else if (o1.versionNumber.compareTo(o2.versionNumber) > 0) { return 1;  }
        // o1.versionNumber.equals(o2.versionNumber)

        return 0;
    }
}
