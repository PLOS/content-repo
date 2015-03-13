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

package org.plos.repo.util;


import org.hsqldb.lib.StringUtil;
import org.plos.repo.service.RepoException;

import java.util.UUID;

public class UUIDFormatter {

  /**
   * Return a UUID from the given <code>uuid</code>. It is intend to be used when the string representing the
   * UUID matches the standard representation as described in the {@link java.util.UUID#toString()} method
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
