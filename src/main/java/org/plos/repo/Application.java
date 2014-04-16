package org.plos.repo;

import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spring.scope.RequestContextFilter;
import org.plos.repo.rest.BucketController;
import org.plos.repo.rest.ObjectController;

public class Application extends ResourceConfig {

  /**
   * Register JAX-RS application components.
   */
  public Application() {

    register(BucketController.class);
    register(ObjectController.class);

    register(MultiPartFeature.class);
    register(RequestContextFilter.class);

  }
}
