package org.plos.repo.models.validator;

import org.plos.repo.service.RepoException;

import java.sql.Timestamp;

/**
 * String timestamp validator.
 */
public class TimestampInputValidator {

  /**
   * Verifies if the <code>timeStamp</code> format is valid.
   * @param timeStamp a single String representing a timestamp to be validated
   * @param errorType RepoException.Type to be thrown if the timestamp is no valid
   * @throws RepoException
   */
  public void validate(String timeStamp, RepoException.Type errorType) throws RepoException {
    if (timeStamp != null) {
      try {
        Timestamp.valueOf(timeStamp);
      } catch (IllegalArgumentException e) {
        throw new RepoException(errorType);
      }
    }
  }

}
