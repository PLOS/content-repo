package org.plos.repo;

import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spring.scope.RequestContextFilter;

public class JerseyApplication extends ResourceConfig {

  /**
   * Register JAX-RS application components.
   */
  public JerseyApplication() {

    packages("org.plos.repo.rest");

    register(MultiPartFeature.class);
    //register(MultiPartMediaTypes.class);
    register(RequestContextFilter.class);
  }
}
