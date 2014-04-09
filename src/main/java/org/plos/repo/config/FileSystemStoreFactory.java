package org.plos.repo.config;

import org.plos.repo.service.FileSystemStoreService;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import java.util.Hashtable;

public class FileSystemStoreFactory implements ObjectFactory {

  public static final String DATA_DIR_PARAM = "dataDirectory";

  public Object getObjectInstance(Object o, Name name, Context context, Hashtable<?, ?> hashtable) throws Exception {
    String dataDirectory = (String) ((Reference) o).get(DATA_DIR_PARAM).getContent();
    return new FileSystemStoreService(dataDirectory);
  }
}
