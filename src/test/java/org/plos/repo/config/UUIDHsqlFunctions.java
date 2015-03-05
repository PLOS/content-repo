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

package org.plos.repo.config;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.UnsupportedEncodingException;
import java.sql.Blob;
import java.sql.SQLException;

/**
 * Created by lmasola on 3/5/15.
 */
public class UUIDHsqlFunctions {

  public static byte[] UNHEX(String uuid) {
    uuid = uuid.replace("-", "");
    char[] hexUuid = Hex.encodeHex(uuid.getBytes());
    try {
      return Hex.decodeHex(hexUuid);
    } catch (DecoderException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static String HEX(Blob uuid) throws DecoderException, UnsupportedEncodingException, SQLException {

    String test1 = uuid.toString();
    byte[] uuidBytes = uuid.getBytes(0, (int) uuid.length());
    String test2 = new String(uuidBytes, "UTF-8");

    return new String(uuidBytes, "UTF-8");

  }

}