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

import org.plos.repo.models.RepoCollection;
import org.plos.repo.models.RepoObject;
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

  public ChecksumGenerator() {
  }

  public String generateVersionChecksum(RepoCollection repoCollection, List<String> objectsChecksum) throws RepoException {
    Collections.sort(objectsChecksum);

    StringBuilder sb = new StringBuilder();

    for (String checksum : objectsChecksum) {
      sb.append(checksum);
    }

    sb.append(repoCollection.getKey());
    sb.append(TimestampFormatter.getFormattedTimestamp(repoCollection.getCreationDate()));
    if (repoCollection.getTag() != null) {
      sb.append(repoCollection.getTag());
    }

    return checksumToString(this.digest(sb.toString()));
  }

  public String generateVersionChecksum(RepoObject repoObject) throws RepoException {
    StringBuilder sb = new StringBuilder();

    sb.append(repoObject.getKey());
    sb.append(TimestampFormatter.getFormattedTimestamp(repoObject.getCreationDate()));
    if (repoObject.getTag() != null) {
      sb.append(repoObject.getTag());
    }
    if (repoObject.getContentType() != null) {
      sb.append(repoObject.getContentType());
    }
    if (repoObject.getDownloadName() != null) {
      sb.append(repoObject.getDownloadName());
    }
    if (repoObject.getUserMetadata() != null) {
      sb.append(repoObject.getUserMetadata());
    }

    sb.append(repoObject.getChecksum());

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

    for (byte b : checksum) {
      sb.append(Integer.toHexString((b & 0xFF) | 0x100).substring(1, 3));
    }

    return sb.toString();
  }

  private byte[] digest(String message) throws RepoException {
    MessageDigest messageDigest = getDigestMessage();
    messageDigest.update(message.getBytes());
    return messageDigest.digest();
  }

}

