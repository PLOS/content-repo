/*
 * Copyright (c) 2006-2014 by Public Library of Science
 * http://plos.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.plos.repo.config;

import org.plos.repo.service.FileSystemStoreService;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import java.util.Hashtable;

public class FileSystemStoreFactory implements ObjectFactory {

  public static final String DATA_DIR_PARAM = "dataDirectory";

  public static final String REPROXY_BASE_URL = "reproxyBaseUrl";

  public Object getObjectInstance(Object o, Name name, Context context, Hashtable<?, ?> hashtable) throws Exception {
    String dataDirectory = (String) ((Reference) o).get(DATA_DIR_PARAM).getContent();
    String reproxyBaseUrl = null;

    if (((Reference) o).get(REPROXY_BASE_URL) != null)
      reproxyBaseUrl = (String) ((Reference) o).get(REPROXY_BASE_URL).getContent();

    return new FileSystemStoreService(dataDirectory, reproxyBaseUrl);
  }
}
