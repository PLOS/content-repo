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

package org.plos.repo.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class HsqlService extends SqlService {

  private static final Logger log = LoggerFactory.getLogger(HsqlService.class);

  @PreDestroy
  public void destroy() throws Exception {
    // kludge for dealing with HSQLDB pooling and unit tests since shutdown=true does not fire

    Connection connection = dataSource.getConnection();
    PreparedStatement p = connection.prepareStatement("CHECKPOINT");
    p.execute();

    p.close();
    connection.close();
  }

  public void postDbInit() throws SQLException {
    // kludges for dealing with HSQLDB

    Connection connection = dataSource.getConnection();
    PreparedStatement p = connection.prepareStatement("select * from INFORMATION_SCHEMA.SYSTEM_INDEXINFO where INDEX_NAME = 'OBJKEYINDEX'");
    ResultSet result = p.executeQuery();

    PreparedStatement pc = null;

    if (result.next()) {
      pc = connection.prepareStatement("CREATE INDEX objKeyIndex ON objects(bucketId, objKey)");
    } else {
      log.info("Creating DB index");
      pc = connection.prepareStatement("CHECKPOINT DEFRAG");
    }

    pc.execute();

    p.close();
    pc.close();
    connection.close();
  }

}
