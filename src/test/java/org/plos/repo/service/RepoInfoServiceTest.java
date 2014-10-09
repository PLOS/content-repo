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
    buckets.add(new org.plos.repo.models.Bucket(1, NAME_BUCKET1, null, null));
    buckets.add(new org.plos.repo.models.Bucket(2, NAME_BUCKET2, null, null));
    when(repoService.listBuckets()).thenReturn(buckets);

    when(repoService.getBucketsSize(1)).thenReturn(SIZE_BUCKET1);
    when(repoService.getBucketsSize(2)).thenReturn(SIZE_BUCKET2);

    ServiceStatus status = repoInfoService.getStatus();

    assertEquals(2, status.bucketCount);
    assertNotNull(status.bucketsSize);
    assertEquals(2, status.bucketsSize.size());
    assertEquals(SIZE_BUCKET1, status.bucketsSize.get(0).getBytes());
    assertEquals(NAME_BUCKET1, status.bucketsSize.get(0).getBucketName());
    assertEquals(SIZE_BUCKET2, status.bucketsSize.get(1).getBytes());
    assertEquals(NAME_BUCKET2, status.bucketsSize.get(1).getBucketName());

    verify(repoService).listBuckets();
    verify(repoService).getBucketsSize(1);
    verify(repoService).getBucketsSize(2);

  }

}
