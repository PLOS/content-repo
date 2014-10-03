package org.plos.repo.util;

import org.plos.repo.models.*;
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

  public String generateVersionChecksum(Collection collection, List<String> objectsChecksum) throws RepoException {

    Collections.sort(objectsChecksum);

    StringBuilder sb = new StringBuilder();

    for (String checksum : objectsChecksum){
      sb.append(checksum);
    }

    sb.append(collection.getKey());
    sb.append(collection.getTimestamp());
    sb.append(collection.getCreationDate());
    sb.append(collection.getStatus().getValue());
    if (collection.getTag() != null){
      sb.append(collection.getTag());
    }

    return checksumToString(this.digest(sb.toString()));
  }

  public String generateVersionChecksum(org.plos.repo.models.Object object) throws RepoException {

    StringBuilder sb = new StringBuilder();

    sb.append(object.getKey());
    sb.append(object.getTimestamp().toString());
    sb.append(object.getCreationDate().toString());
    sb.append(object.getTag());
    sb.append(object.getContentType());
    sb.append(object.getDownloadName());
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

