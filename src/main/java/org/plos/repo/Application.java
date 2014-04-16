package org.plos.repo;

import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spring.scope.RequestContextFilter;
import org.plos.repo.rest.BucketController;
import org.plos.repo.rest.ObjectCrudController;

public class Application extends ResourceConfig {

  /**
   * Register JAX-RS application components.
   */
  public Application() {

//    register(SpringComponentProvider.class);
//    register(SpringLifecycleListener.class);
//
//    packages("org.plos.repo");

    register(BucketController.class);
    register(ObjectCrudController.class);

    register(MultiPartFeature.class);
    register(RequestContextFilter.class);

  }
}
