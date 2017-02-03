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

package org.plos.repo.models.validator;

import org.plos.repo.service.RepoException;

import java.sql.Timestamp;

/**
 * String timestamp validator.
 */
public class TimestampInputValidator {

  /**
   * Verifies if the <code>timeStamp</code> format is valid.
   *
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
