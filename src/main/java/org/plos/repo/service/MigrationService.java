package org.plos.repo.service;

import org.plos.repo.models.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

/**
 * Handles the communication for migration with sqlservice
 */
public class MigrationService extends BaseRepoService{

  private static final Logger log = LoggerFactory.getLogger(MigrationService.class);

  @Inject
  private TimestampInputValidator timestampInputValidator;

  public List<Operation> listHistoricOperations(String timestampString, Integer offset, Integer limit) throws RepoException{

    validatePagination(offset, limit);
    timestampInputValidator.validate(timestampString, RepoException.Type.CouldNotParseTimestamp);
    List<Bucket> buckets = null;
    List<org.plos.repo.models.Object> objects = null;
    List<Collection> collections = null;
    try {
      sqlService.getConnection();
      if (timestampString != null) {
        Timestamp timestamp = Timestamp.valueOf(timestampString);
        buckets = sqlService.listBuckets();   // TODO: add timestamp on buckets table and create the appropiate query
        objects = sqlService.listObjects(timestamp);
        collections = sqlService.listCollections(timestamp);
      } else {
        buckets = sqlService.listBuckets();
        objects = sqlService.listObjects(null, offset, limit, true);
        collections = sqlService.listCollections(null, offset, limit, true, null);
      }

      return null;

    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {
      sqlReleaseConnection();
    }
  }

  @Override
  public Logger getLog() {
    return log;
  }
}
