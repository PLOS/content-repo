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

package org.plos.repo.model.output;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.plos.repo.models.Audit;
import org.plos.repo.models.Operation;
import org.plos.repo.models.output.RepoAuditOutput;

import java.sql.Timestamp;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * Created by lmasola on 14/04/15.
 */
public class RepoAuditOutputTest {

  private static final String VALID_KEY = "valid-key";
  private static final String VALID_BUCKET_NAME = "valid-bucket-name";
  private static final Timestamp VALID_TIMESTAMP = Timestamp.valueOf("2014-09-02 1:55:32");
  private static final java.util.UUID VALID_UUID = UUID.randomUUID();
  private static final Operation OPERATION = Operation.CREATE_COLLECTION;

  @Mock
  private Audit audit;

  @Before
  public void setUp() {
    initMocks(this);
  }

  @Test
  public void creationTest() {

    when(audit.getKey()).thenReturn(VALID_KEY);
    when(audit.getBucket()).thenReturn(VALID_BUCKET_NAME);
    when(audit.getTimestamp()).thenReturn(VALID_TIMESTAMP);
    when(audit.getOperation()).thenReturn(Operation.CREATE_COLLECTION);
    when(audit.getUuid()).thenReturn(VALID_UUID);

    RepoAuditOutput repoAuditOutput = new RepoAuditOutput(audit);
    assertEquals(VALID_KEY, repoAuditOutput.getKey());
    assertEquals(VALID_BUCKET_NAME, repoAuditOutput.getBucket());
    assertEquals(VALID_TIMESTAMP, repoAuditOutput.getTimestamp());
    assertEquals(VALID_UUID.toString(), repoAuditOutput.getUuid());
    assertEquals(VALID_BUCKET_NAME, repoAuditOutput.getBucket());

  }

  @Test
  public void creationWithEmptyFieldsTest() {

    when(audit.getBucket()).thenReturn(VALID_BUCKET_NAME);
    when(audit.getTimestamp()).thenReturn(VALID_TIMESTAMP);
    when(audit.getOperation()).thenReturn(Operation.CREATE_COLLECTION);

    RepoAuditOutput repoAuditOutput = new RepoAuditOutput(audit);
    assertNull(repoAuditOutput.getKey());
    assertEquals(VALID_BUCKET_NAME, repoAuditOutput.getBucket());
    assertEquals(VALID_TIMESTAMP, repoAuditOutput.getTimestamp());
    assertNull(repoAuditOutput.getUuid());
    assertEquals(VALID_BUCKET_NAME, repoAuditOutput.getBucket());

  }

}
