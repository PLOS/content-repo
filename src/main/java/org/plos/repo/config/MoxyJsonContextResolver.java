package org.plos.repo.config;

import org.glassfish.jersey.moxy.json.MoxyJsonConfig;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import java.util.HashMap;
import java.util.Map;

@Provider
public class MoxyJsonContextResolver implements ContextResolver<MoxyJsonConfig> {

  private final MoxyJsonConfig config;

  public MoxyJsonContextResolver() {
    final Map<String, String> namespacePrefixMapper = new HashMap<>();
    namespacePrefixMapper.put("http://www.w3.org/2001/XMLSchema-instance", "xsi");

    config = new MoxyJsonConfig()
        .setNamespacePrefixMapper(namespacePrefixMapper)
        .setNamespaceSeparator(':');
  }

  @Override
  public MoxyJsonConfig getContext(Class<?> objectType) {
    return config;
  }

}
