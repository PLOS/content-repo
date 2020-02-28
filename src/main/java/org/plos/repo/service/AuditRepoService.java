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

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import com.google.common.util.concurrent.Striped;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.plos.repo.models.Audit;

/**
 * This service handles all communication to sqlService for audit services
 */
public class AuditRepoService extends BaseRepoService {

  private Striped<ReadWriteLock> rwLocks = Striped.lazyWeakReadWriteLock(32);

  private static final Logger log = LogManager.getLogger(AuditRepoService.class);

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

      return sqlService.listAuditRecords(offset, limit);

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
