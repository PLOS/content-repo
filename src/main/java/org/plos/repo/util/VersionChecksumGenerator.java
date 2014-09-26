package org.plos.repo.util;

import org.plos.repo.models.*;

import java.util.Collections;
import java.util.List;

/**
 * Checksum generator.
 */
public class VersionChecksumGenerator {

  public String generateVersionChecksum(Collection collection, List<String> objectsChecksum) {

    Collections.sort(objectsChecksum);

    StringBuilder sb = new StringBuilder();

    for (String checksum : objectsChecksum){
      sb.append(checksum);
    }

    sb.append(collection.getTimestamp());
    sb.append(collection.getCreationDate());
    sb.append(collection.getStatus().getValue());
    if (collection.getTag() != null){
      sb.append(collection.getTag());
    }

    Integer hashCode = sb.toString().hashCode();

    return hashCode.toString();
  }

  public String generateVersionChecksum(org.plos.repo.models.Object object) {

    StringBuilder sb = new StringBuilder();

    sb.append(object.timestamp.toString());
    sb.append(object.creationDate.toString());
    sb.append(object.tag);
    sb.append(object.contentType);
    sb.append(object.downloadName);
    sb.append(object.checksum);

    Integer hashCode = sb.toString().hashCode();

    return hashCode.toString();
  }

}

