package org.plos.repo.models;

import org.plos.repo.service.RepoException;

import java.sql.Timestamp;

/**
 * Input Collection Validator. It validates the required fields.
 */
public class InputCollectionValidator {

  public void validate(InputCollection collection) throws RepoException {

    if (collection.getKey() == null)
      throw new RepoException(RepoException.Type.NoCollectionKeyEntered);

    if (collection.getBucketName() == null)
      throw new RepoException(RepoException.Type.NoBucketEntered);

    validateTimeStamp(collection.getTimestampString(), RepoException.Type.CouldNotParseTimestamp);
    validateTimeStamp(collection.getCreationDateTimeString(), RepoException.Type.CouldNotParseCreationDate);


    if (collection.getObjects() == null || collection.getObjects().size() == 0 ) {
      throw new RepoException(RepoException.Type.CantCreateCollectionWithNoObjects);
    }

  }


  private void validateTimeStamp(String timeStamp, RepoException.Type errorType) throws RepoException {
    if (timeStamp != null) {
      try {
        Timestamp.valueOf(timeStamp);
      } catch (IllegalArgumentException e) {
        throw new RepoException(errorType);
      }
    }
  }

}
