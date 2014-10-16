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
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.plos.repo.models.output.Bucket;
import org.plos.repo.models.output.ServiceStatus;

import javax.xml.bind.annotation.XmlTransient;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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
  public void setUp(){
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
