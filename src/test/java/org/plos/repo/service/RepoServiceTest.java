package org.plos.repo.service;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.plos.repo.RepoBaseTest;
import org.plos.repo.models.Bucket;

import java.lang.reflect.Field;

public class RepoServiceTest extends RepoBaseTest {

  private RepoService repoService;

  @Before
  public void setup() {
    repoService = context.getBean(RepoService.class);
  }

  @Test
  public void createBucketRollbackTest() throws Exception {

    ObjectStore mockObjectStore = Mockito.mock(ObjectStore.class);

    Mockito.when(mockObjectStore.createBucket(Mockito.any(Bucket.class))).thenThrow(new RepoException(RepoException.Type.ClientError, "error creating bucket in store"));

    Field objStoreField = RepoService.class.getDeclaredField("objectStore");
    objStoreField.setAccessible(true);
    objStoreField.set(repoService, mockObjectStore);

    try {
      repoService.createBucket("bucket1");
      Assert.fail();
    } catch (RepoException e) {
      Assert.assertTrue(e.getType() == RepoException.Type.ClientError);
    }

    //assert db state


  }

}
