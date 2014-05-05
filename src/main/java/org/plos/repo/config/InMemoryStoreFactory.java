package org.plos.repo.config;


import javax.naming.Context;
import javax.naming.Name;
import javax.naming.spi.ObjectFactory;
import java.util.Hashtable;

public class InMemoryStoreFactory implements ObjectFactory {

  public Object getObjectInstance(Object o, Name name, Context context, Hashtable<?, ?> hashtable) throws Exception {

    return new org.plos.repo.service.InMemoryFileStoreService();
  }

}

