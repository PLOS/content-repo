package org.plos.repo.util;

import org.plos.repo.models.Collection;
import org.plos.repo.models.TimestampFormatter;
import org.plos.repo.service.RepoException;

import javax.inject.Inject;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * Checksum generator.
 */
public class ChecksumGenerator {

  private static final String DIGEST_ALGORITHM = "SHA-1";

  private TimestampFormatter timestampFormatter;

  public ChecksumGenerator(){
    timestampFormatter = new TimestampFormatter();
  }

  public String generateVersionChecksum(Collection collection, List<String> objectsChecksum) throws RepoException {

    Collections.sort(objectsChecksum);

    StringBuilder sb = new StringBuilder();

    for (String checksum : objectsChecksum){
      sb.append(checksum);
    }

    sb.append(collection.getKey());
    sb.append(timestampFormatter.getFormattedTimestamp(collection.getCreationDate()));
    if (collection.getTag() != null){
      sb.append(collection.getTag());
    }

    return checksumToString(this.digest(sb.toString()));
  }

  public String generateVersionChecksum(org.plos.repo.models.Object object) throws RepoException {

    StringBuilder sb = new StringBuilder();

    sb.append(object.getKey());
    sb.append(timestampFormatter.getFormattedTimestamp(object.getCreationDate()));
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

