/*
 * Copyright (c) 2006-2015 by Public Library of Science
 *
 *    http://plos.org
 *    http://ambraproject.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.plos.repo.service;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Striped;
import org.hsqldb.lib.StringUtil;
import org.plos.repo.models.*;
import org.plos.repo.models.input.ElementFilter;
import org.plos.repo.models.input.InputRepoObject;
import org.plos.repo.models.validator.InputRepoObjectValidator;
import org.plos.repo.models.validator.TimestampInputValidator;
import org.plos.repo.util.UUIDFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * This service handles all communication to sqlService for audit services
 */
public class AuditRepoService extends BaseRepoService {

  private Striped<ReadWriteLock> rwLocks = Striped.lazyWeakReadWriteLock(32);

  private static final Logger log = LoggerFactory.getLogger(AuditRepoService.class);

  /**
   * List audit records order by creation date, using limit and offset to paginate the response
   * @param offset         a single number used to paginate the response
   * @param limit          a single number used to paginate the response, indicating the limit of rows returned

   * @return a list of audit records {@link org.plos.repo.models.Audit}
   * @throws org.plos.repo.service.RepoException if a server error occurs
   */
  public List<Audit> listAuditRecords(Integer offset, Integer limit) throws RepoException {

    if (offset == null) {
      offset = 0;
    }
    if (limit == null) {
      limit = DEFAULT_PAGE_SIZE;
    }

    try {
      validatePagination(offset, limit);

      sqlService.getReadOnlyConnection();

      return sqlService.listAuditRegisters(offset, limit);
    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {
      sqlReleaseConnection();
    }
  }

  @Override
  public Logger getLog() {
    return log;
  }

}
