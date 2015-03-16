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


import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class UUIDFormatterTest {


  private static final String OBJECT_KEY_COLUMN = "OBJKEY";
  private static final String COLLECTION_KEY_COLUMN = "COLLKEY";
  private static final String BUCKET_ID_COLUMN = "BUCKETID";
  private static final String BUCKET_NAME_COLUMN = "BUCKETNAME";
  private static final String STATUS_COLUMN = "STATUS";
  private static final String ID_COLUMN = "ID";
  private static final String CHECKSUM_COLUMN = "CHECKSUM";
  private static final String TIMESTAMP_COLUMN = "TIMESTAMP";
  private static final String TAG_COLUMN = "TAG";
  private static final String VERSION_NUMBER_COLUMN = "VERSIONNUMBER";
  private static final String CREATION_DATE_COLUMN = "CREATIONDATE";
  private static final String USER_METADATA_COLUMN = "USERMETADATA";
  private static final String UUID_COLUMN = "UUID";
  private static final String HEX_UUID_COLUMN = "HEX_UUID";
  private static final String DOWNLOAD_NAME_COLUMN = "DOWNLOADNAME";
  private static final String CONTENT_TYPE_COLUMN = "CONTENTTYPE";
  private static final String SIZE_COLUMN = "SIZE";

  private static String OBJECT_COLUMNS = "obj." + OBJECT_KEY_COLUMN + ",obj." + BUCKET_ID_COLUMN
      + ",obj." + STATUS_COLUMN + ",obj." + ID_COLUMN + ",obj." + CHECKSUM_COLUMN
      + ",obj." + TIMESTAMP_COLUMN + ",obj." + DOWNLOAD_NAME_COLUMN + ",obj." + CONTENT_TYPE_COLUMN
      + ",obj." + SIZE_COLUMN + ",obj." + TAG_COLUMN + ",obj." + VERSION_NUMBER_COLUMN + ",obj." + CREATION_DATE_COLUMN
      + ",obj." + USER_METADATA_COLUMN + ", HEX(obj.uuid )";

  @Test
  public void test() throws DecoderException, UnsupportedEncodingException {

    String str = "406699C2D5EF4941882D854B817E882C";

    str = str.replaceAll("(\\w{8})(\\w{4})(\\w{4})(\\w{4})(\\w{12})", "$1-$2-$3-$4-$5");

    assertTrue(str.contains("-"));

    String myString = UUID.randomUUID().toString();
    myString = myString.replace("-", "");
    char[] hexString = Hex.encodeHex(myString.getBytes());
    assertNotNull(hexString);
    byte[] uuid = Hex.decodeHex(hexString);

    String decoded = new String(uuid, "UTF-8");
    assertEquals(decoded, myString);


  }

  @Test
  public void test1() {

    StringBuilder q = new StringBuilder();
    q.append("SELECT " + OBJECT_COLUMNS + "  FROM objects as obj, buckets b " +
        "WHERE obj.bucketId = b.bucketId");

    String p = q.toString();

    assertNotNull(p);

  }

}
