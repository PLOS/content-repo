package org.plos.repo.models;

import org.plos.repo.service.RepoException;

import javax.inject.Inject;
import java.sql.Timestamp;

/**
 * Input Collection Validator. It validates the required fields.
 */
public class InputCollectionValidator {

  @Inject
  private TimestampInputValidator timestampValidator;

  public void validate(InputCollection collection) throws RepoException {

    if (collection.getKey() == null)
      throw new RepoException(RepoException.Type.NoCollectionKeyEntered);

    if (collection.getBucketName() == null)
      throw new RepoException(RepoException.Type.NoBucketEntered);

    timestampValidator.validate(collection.getTimestampString(), RepoException.Type.CouldNotParseTimestamp);
    timestampValidator.validate(collection.getCreationDateTimeString(), RepoException.Type.CouldNotParseCreationDate);


    if (collection.getObjects() == null || collection.getObjects().size() == 0 ) {
      throw new RepoException(RepoException.Type.CantCreateCollectionWithNoObjects);
    }

  }

}
