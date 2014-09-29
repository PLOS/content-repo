package org.plos.repo.util;

import org.plos.repo.models.*;

import java.util.Collections;
import java.util.List;

/**
 * Checksum generator.
 */
public class VersionChecksumGenerator {

  public Integer generateVersionChecksum(Collection collection, List<Integer> objectsChecksum) {

    Collections.sort(objectsChecksum);

    StringBuilder sb = new StringBuilder();

    for (Integer checksum : objectsChecksum){
      sb.append(checksum);
    }

    sb.append(collection.getTimestamp());
    sb.append(collection.getCreationDate());
    sb.append(collection.getStatus().getValue());
    if (collection.getTag() != null){
      sb.append(collection.getTag());
    }

    return sb.toString().hashCode();
  }

  public Integer generateVersionChecksum(org.plos.repo.models.Object object) {

    StringBuilder sb = new StringBuilder();

    sb.append(object.timestamp.toString());
    sb.append(object.creationDate.toString());
    sb.append(object.tag);
    sb.append(object.contentType);
    sb.append(object.downloadName);
    sb.append(object.checksum);

    return sb.toString().hashCode();
  }

}

