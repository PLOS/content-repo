package org.plos.repo;

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.plos.repo.models.Bucket;
import org.plos.repo.service.InMemoryFileStoreService;

import javax.inject.Inject;

@RunWith(Arquillian.class)
public class InMemoryStoreArquillianTest {

  @Deployment
  public static JavaArchive createDeployment() {
    JavaArchive jar = ShrinkWrap.create(JavaArchive.class)
        .addClass(InMemoryFileStoreService.class)
        .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");

    System.out.println(jar.toString(true));
    return jar;
  }

  @Inject
  InMemoryFileStoreService inMemoryFileStoreService;


  @Test
  public void non_null() {
    Assert.assertNotNull(inMemoryFileStoreService);
    Assert.assertFalse(inMemoryFileStoreService.hasXReproxy());
  }

  @Test
  public void create_bucket() {
    Assert.assertTrue(inMemoryFileStoreService.createBucket(new Bucket(1, "one")));
    Assert.assertFalse(inMemoryFileStoreService.createBucket(new Bucket(1, "one")));
  }

}
