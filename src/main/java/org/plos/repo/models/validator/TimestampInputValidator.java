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

package org.plos.repo.models.validator;

import org.plos.repo.service.RepoException;

import java.sql.Timestamp;

/**
 * String timestamp validator.
 */
public class TimestampInputValidator {

  /**
   * Verifies if the <code>timeStamp</code> format is valid.
   * @param timeStamp a single String representing a timestamp to be validated
   * @param errorType RepoException.Type to be thrown if the timestamp is no valid
   * @throws RepoException
   */
  public void validate(String timeStamp, RepoException.Type errorType) throws RepoException {
    if (timeStamp != null) {
      try {
        Timestamp.valueOf(timeStamp);
      } catch (IllegalArgumentException e) {
        throw new RepoException(errorType);
      }
    }
  }

}
