package org.plos.repo.service;

import org.plos.repo.models.*;
import org.plos.repo.models.Object;
import org.plos.repo.models.validator.TimestampInputValidator;
import org.plos.repo.util.OperationComparator;
import org.plos.repo.util.SortedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

/**
 * Handles the communication for migration services with sqlservice
 */
public class MigrationService extends BaseRepoService{

  private static final Logger log = LoggerFactory.getLogger(MigrationService.class);

  @Inject
  private TimestampInputValidator timestampInputValidator;

  @Inject
  private OperationComparator operationComparator;

  public SortedList<Operation> listHistoricOperations(String timestampString, Integer offset, Integer limit) throws RepoException{

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
        objects = sqlService.listObjects(null, offset, limit, true, null);
        collections = sqlService.listCollections(null, offset, limit, true, null);
      }

      return createOperationList(buckets, objects, collections);

    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {
      sqlReleaseConnection();
    }
  }

  private SortedList<Operation> createOperationList(List<Bucket> buckets, List<Object> objects, List<Collection> collections) {

    SortedList<Operation> operations = new SortedList<Operation>(operationComparator);

    for (Bucket bucket : buckets){
      Operation o = new Operation(bucket);
      operations.add(o);
    }
    for (Object object : objects) {
      if (Status.USED.equals(object.getStatus())) {
        Operation o = new Operation(object, Status.USED);
        operations.add(o);
      } else {
        Operation o1 = new Operation(object, Status.USED);
        Operation o2 = new Operation(object, Status.DELETED);
        operations.add(o1);
        operations.add(o2);
      }
    }
    for (Collection collection : collections){
      if (Status.USED.equals(collection.getStatus())){
        Operation o = new Operation(collection, Status.USED);
        operations.add(o);
      } else {
        Operation o1 = new Operation(collection, Status.USED);
        Operation o2 = new Operation(collection, Status.DELETED);
        operations.add(o1);
        operations.add(o2);
      }
    }

    return operations;

  }

  @Override
  public Logger getLog() {
    return log;
  }
}
