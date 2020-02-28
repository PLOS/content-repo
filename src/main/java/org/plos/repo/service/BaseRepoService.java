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
import javax.inject.Inject;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;
import org.plos.repo.models.Audit;

/**
 * This service handles all communication for collections with sqlservice
 */
public abstract class BaseRepoService {

  // default page size = number of objects returned when no limit= parameter supplied.
  protected static final Integer DEFAULT_PAGE_SIZE = 1000;

  // maximum allowed value of page size, i.e., limit= parameter
  protected static final Integer MAX_PAGE_SIZE = 10000;

  public enum CreateMethod {
    NEW, VERSION, AUTO;
  }

  @Inject
  protected SqlService sqlService;

  protected void sqlReleaseConnection() throws RepoException {
    try {
      sqlService.releaseConnection();
    } catch (SQLException e) {
      throw new RepoException(e);
    }
  }

  protected void sqlRollback(String data) throws RepoException {
    getLog().error("DB rollback: " + data + "\n" +
        StringUtils.join(Thread.currentThread().getStackTrace(), "\n\t"));

    try {
      sqlService.transactionRollback();
    } catch (SQLException e) {
      throw new RepoException(e);
    }
  }


  protected void validatePagination(Integer offset, Integer limit) throws RepoException {
    if (offset < 0) {
      throw new RepoException(RepoException.Type.InvalidOffset);
    }

    if (limit <= 0 || limit > MAX_PAGE_SIZE) {
      throw new RepoException(RepoException.Type.InvalidLimit);
    }
  }

  /**
   * This method audit all the others services operations
   *
   * @param audit contains the operation's information to audit
   * @throws RepoException if there is a error saving the audit row
   */
  protected void auditOperation(Audit audit) throws RepoException {

    try {
      boolean result = sqlService.insertAudit(audit);

      if (!result) {
        throw new RepoException("Error saving audit operation to database " + audit);
      }
    } catch (SQLException e) {
      getLog().error("Exception: {} when trying to save audit operation {}",
          e.getMessage(),
          audit.toString());
      throw new RepoException("Exception: " + e.getMessage() + " when trying to save audit operation " + audit);
    }
  }

  public abstract Logger getLog();

}
