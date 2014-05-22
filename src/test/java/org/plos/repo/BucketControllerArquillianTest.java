package org.plos.repo;

import junit.framework.Assert;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Test;

public class BucketControllerArquillianTest {

  @Deployment
  public static JavaArchive createDeployment() {

    JavaArchive archive = ShrinkWrap.create(JavaArchive.class,
        "myPackage.jar").addPackage(JerseyApplication.class.getPackage());

    System.out.println("archive ... " + archive.toString(true));
    return archive;
  }

//  @Inject
//  InMemoryFileStoreService inMemoryFileStoreService;
//

  @Test
  public void test() {
    Assert.fail("no tests written");
  }

}
