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
package org.plos.repo.service;

import org.junit.Before;
import org.junit.Test;
import org.plos.repo.RepoBaseSpringTest;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ThreadLocalTest extends RepoBaseSpringTest {

  private ThreadLocal<Connection> threadLocalHnd;

  private CountDownLatch startGate;
  private CountDownLatch endGate;

  @Before
  @SuppressWarnings("unchecked")
  public void setup() throws Exception {
    Field threadLocalField = SqlService.class.getDeclaredField("connectionLocal");
    threadLocalField.setAccessible(true);
    threadLocalHnd = (ThreadLocal<Connection>) threadLocalField.get(this.sqlService);

    this.startGate = new CountDownLatch(1);
  }

  @Test
  public void testConnectionEquality() throws Exception {
    Connection conn = threadLocalHnd.get();
    Connection conn2 = threadLocalHnd.get();
    assertEquals(conn, conn2);
  }

  @Test
  public void testThreadLocalSingleThread() throws Exception {
    // ExistingConnection : GET
    Connection conn = threadLocalHnd.get();
    assertNotNull(conn);

    // ExistingConnection : REMOVE
    this.sqlService.releaseConnection();
    assertNull(threadLocalHnd.get());

    // make sure safe to call release multiple times
    this.sqlService.releaseConnection();
    assertNull(threadLocalHnd.get());

    // NoConnection: REMOVE (no-op?)
    threadLocalHnd.remove();

    // NoConnection : GET
    conn = threadLocalHnd.get();
    assertNull(conn);

    // NoConnection : SET
    this.sqlService.getConnection();
    Connection conn1 = threadLocalHnd.get();
    assertNotNull(conn1);

    // ExistingConnection : SET (overwrites)
    this.sqlService.getConnection();
    Connection conn2 = threadLocalHnd.get();
    assertTrue(conn1 != conn2);

    // Be a good citizen and close connections before removing as thread locals
    conn1.close();
    conn2.close();

    threadLocalHnd.remove();
    assertNull(threadLocalHnd.get());
  }

  @Test
  public void testThreadLocalMultipleThreads() throws Exception {
    final int THREADS = 10;

    this.endGate = new CountDownLatch(THREADS);

    final Set<Connection> connections = Collections.synchronizedSet(new HashSet<>());

    for (int i = 0; i < THREADS; i++) {
      final Thread t = new Thread() {
        public void run() {
          try {
            startGate.await();
            try {
              sqlService.getReadOnlyConnection();
              connections.add(threadLocalHnd.get());
              sqlService.releaseConnection();
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

    startGate.countDown(); // start all client threads
    endGate.await();       // wait until all threads have finished (L=0)

    assertEquals(THREADS, connections.size());
    for (Connection c : connections) {
      assertTrue(c.isClosed());
    }
  }

}
