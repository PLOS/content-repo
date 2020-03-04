/*
 * Copyright (c) 2017 Public Library of Science
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

package org.plos.repo.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HsqlService extends SqlService {

  private static final Logger log = LogManager.getLogger(HsqlService.class);

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
