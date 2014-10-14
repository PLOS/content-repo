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

package org.plos.repo.util;

import org.plos.repo.models.Collection;
import org.plos.repo.service.RepoException;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;

/**
 * Checksum generator.
 */
public class ChecksumGenerator {

  private static final String DIGEST_ALGORITHM = "SHA-1";

  public ChecksumGenerator(){
  }

  public String generateVersionChecksum(Collection collection, List<String> objectsChecksum) throws RepoException {

    Collections.sort(objectsChecksum);

    StringBuilder sb = new StringBuilder();

    for (String checksum : objectsChecksum){
      sb.append(checksum);
    }

    sb.append(collection.getKey());
    sb.append(TimestampFormatter.getFormattedTimestamp(collection.getCreationDate()));
    if (collection.getTag() != null){
      sb.append(collection.getTag());
    }

    return checksumToString(this.digest(sb.toString()));
  }

  public String generateVersionChecksum(org.plos.repo.models.Object object) throws RepoException {

    StringBuilder sb = new StringBuilder();

    sb.append(object.getKey());
    sb.append(TimestampFormatter.getFormattedTimestamp(object.getCreationDate()));
    if (object.getTag() != null){
      sb.append(object.getTag());
    }
    if (object.getContentType() != null){
      sb.append(object.getContentType());
    }
    if (object.getDownloadName() != null ){
      sb.append(object.getDownloadName());
    }

    sb.append(object.getChecksum());

    return checksumToString(this.digest(sb.toString()));
  }

  public MessageDigest getDigestMessage() throws RepoException {

    try {
      MessageDigest messageDigest = MessageDigest.getInstance(DIGEST_ALGORITHM);
      messageDigest.reset();
      return messageDigest;
    } catch (NoSuchAlgorithmException e) {
      throw new RepoException(e);
    }


  }

  public String checksumToString(byte[] checksum) {

    StringBuilder sb = new StringBuilder();

    for (byte b : checksum)
      sb.append(Integer.toHexString((b & 0xFF) | 0x100).substring(1,3));

    return sb.toString();
  }

  private byte[] digest(String message) throws RepoException {

      MessageDigest messageDigest = getDigestMessage();
      messageDigest.update(message.getBytes());
      return messageDigest.digest();

  }

}

