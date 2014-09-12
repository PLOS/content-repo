package org.plos.repo.models;

import org.plos.repo.service.RepoException;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

/**
 * Input Collection Validator. It validates the required fields.
 */
public class InputCollectionValidator {

  public void validate(InputCollection collection) throws RepoException {

    if (collection.getKey() == null)
      throw new RepoException(RepoException.Type.NoCollectionKeyEntered);

    if (collection.getBucketName() == null)
      throw new RepoException(RepoException.Type.NoBucketEntered);

    if (collection.getTimestampString() != null) {
      try {
        collection.setTimestamp(Timestamp.valueOf(collection.getTimestampString()));
      } catch (IllegalArgumentException e) {
        throw new RepoException(RepoException.Type.CouldNotParseTimestamp);
      }
    }

    if (collection.getObjects() == null || collection.getObjects().size() == 0 ) {
      throw new RepoException(RepoException.Type.CantCreateCollectionWithNoObjects);
    }

  }


}
