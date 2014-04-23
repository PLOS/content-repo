package org.plos.repo;

import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spring.scope.RequestContextFilter;
import org.plos.repo.rest.BucketController;
import org.plos.repo.rest.ObjectController;
import org.plos.repo.rest.RootController;

public class JerseyApplication extends ResourceConfig {

  /**
   * Register JAX-RS application components.
   */
  public JerseyApplication() {

    register(BucketController.class);
    register(ObjectController.class);
    register(RootController.class);

    register(MultiPartFeature.class);
    register(RequestContextFilter.class);

  }
}
