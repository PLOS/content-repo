/*
 * Copyright (c) 2014-2019 Public Library of Science
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

package org.plos.repo.util;


import org.hsqldb.lib.StringUtil;
import org.plos.repo.service.RepoException;

import java.util.UUID;

public class UUIDFormatter {

  /**
   * Return a UUID from the given <code>uuid</code>. It is intend to be used when the string representing the UUID
   * matches the standard representation as described in the {@link java.util.UUID#toString()} method
   *
   * @param uuid a single String representing the UUID.
   * @return a UUID
   * @throws RepoException
   */
  public static UUID getUuid(String uuid) throws RepoException {
    if (!StringUtil.isEmpty(uuid)) {
      try {
        return UUID.fromString(uuid);
      } catch (IllegalArgumentException e) {
        throw new RepoException(RepoException.Type.InvalidUuid);
      }
    }

    return null;
  }

}
