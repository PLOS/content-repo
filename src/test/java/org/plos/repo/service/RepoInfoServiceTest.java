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
import org.mockito.internal.util.reflection.Whitebox;
import org.plos.repo.models.output.ServiceStatus;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class RepoInfoServiceTest {

  private static String NAME_BUCKET1 = "b1";
  private static String NAME_BUCKET2 = "b2";
  private static Long SIZE_BUCKET1 = 123456l;
  private static Long SIZE_BUCKET2 = 654321l;

  @InjectMocks
  private RepoInfoService repoInfoService;

  @Mock
  private RepoService repoService;

  @Mock
  private List<org.plos.repo.models.Bucket> buckets;

  @Before
  public void setUp() {
    repoInfoService = new RepoInfoService();
    initMocks(this);
    Whitebox.setInternalState(repoInfoService, "startTime", new Date());
  }

  @Test
  public void getStatusHappyPathTest() throws RepoException {
    List<org.plos.repo.models.Bucket> buckets = new ArrayList<>();
    buckets.add(mock(org.plos.repo.models.Bucket.class));
    buckets.add(mock(org.plos.repo.models.Bucket.class));
    when(repoService.listBuckets()).thenReturn(buckets);

    ServiceStatus status = repoInfoService.getStatus();

    assertEquals(2, status.bucketCount);
    assertNotNull(status.serviceStarted);
    assertNotNull(status.readsSinceStart);
    assertNotNull(status.writesSinceStart);

    verify(repoService).listBuckets();
  }

}
