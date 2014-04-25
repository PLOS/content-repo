package org.plos.repo.config;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import java.util.Hashtable;

public class MogileStoreFactory implements ObjectFactory {

  public Object getObjectInstance(Object o, Name name, Context context, Hashtable<?, ?> hashtable) throws Exception {

    return new org.plos.repo.service.MogileStoreService(
        ((Reference) o).get("domain").getContent().toString(),
        ((Reference) o).get("trackers").getContent().toString().split(","),
        Integer.parseInt(((Reference) o).get("maxTrackerConnections").getContent().toString()),
        Integer.parseInt(((Reference) o).get("maxIdleConnections").getContent().toString()),
        Long.parseLong(((Reference) o).get("maxIdleTimeMillis").getContent().toString())
    );
  }
}
