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

import org.apache.commons.lang.StringUtils;
import org.plos.repo.models.Audit;
import org.plos.repo.util.ChecksumGenerator;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.sql.SQLException;

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

  @Inject
  protected ChecksumGenerator checksumGenerator;
  
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
    if (offset < 0)
      throw new RepoException(RepoException.Type.InvalidOffset);

    if (limit <= 0 || limit > MAX_PAGE_SIZE)
      throw new RepoException(RepoException.Type.InvalidLimit);
  }

  /**
   * This method audit all the others services operations  
   * @param audit contains the operation's information to audit
   * @throws RepoException if there is a error saving the audit row
   */
  protected void auditOperation(Audit audit) throws RepoException{
    final boolean result;

    try {
      
      result = sqlService.insertAudit(audit);
      
      if (!result) {
        throw new RepoException("Error saving audit operation to database " + audit);
      }

    } catch (SQLException e){
      getLog().error("Exception: {} when trying to save audit operation {}", 
          e.getMessage(), 
          audit.toString());
      throw new RepoException("Exception: " + e.getMessage() + " when trying to save audit operation " + audit);
    } 
  }
  
  public abstract Logger getLog();

}
