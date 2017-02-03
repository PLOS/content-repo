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

package org.plos.repo.service;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.plos.repo.models.Audit;

import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * Created by lmasola on 14/04/15.
 */
public class AuditRepoServiceTest {

  private static final Integer OFFSET = 0;
  private static final Integer LIMIT = 3;
  private static String NAME_BUCKET1 = "b1";
  private static String NAME_BUCKET2 = "b2";
  private static Long SIZE_BUCKET1 = 123456l;
  private static Long SIZE_BUCKET2 = 654321l;

  @InjectMocks
  private AuditRepoService auditRepoService;

  @Mock
  private SqlService sqlService;

  @Mock
  private List<Audit> expectedAuditRecords;

  @Before
  public void setUp() {
    auditRepoService = new AuditRepoService();
    initMocks(this);
  }

  @Test
  public void getStatusHappyPathTest() throws RepoException, SQLException {

    doNothing().when(sqlService).getReadOnlyConnection();
    when(sqlService.listAuditRecords(OFFSET, LIMIT)).thenReturn(expectedAuditRecords);

    List<Audit> auditResults= auditRepoService.listAuditRecords(OFFSET, LIMIT);

    assertNotNull(auditResults);
    assertEquals(expectedAuditRecords, auditResults);

    verify(sqlService).getReadOnlyConnection();
    verify(sqlService).listAuditRecords(OFFSET, LIMIT);
  }


}
