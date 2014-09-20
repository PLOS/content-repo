package org.plos.repo.models;

import org.plos.repo.service.RepoException;

import java.sql.Timestamp;

/**
 * String timestamp validator
 */
public class TimestampInputValidator {

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
