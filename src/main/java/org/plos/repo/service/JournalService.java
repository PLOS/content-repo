package org.plos.repo.service;



import org.plos.repo.models.Journal;
import org.plos.repo.models.Operation;

import java.sql.SQLException;
import java.util.List;

import org.plos.repo.models.RepoCollection;
import org.plos.repo.models.RepoObject;
import org.plos.repo.models.input.InputCollection;
import org.plos.repo.models.input.InputObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JournalService {

    private static final Logger log = LoggerFactory.getLogger(JournalService.class);

    @javax.inject.Inject
    private SqlService sqlService;
    
    
    public void createBucket(String bucketName){
        final boolean result;
        try {
            result = sqlService.insertJournal(new Journal(bucketName, Operation.CREATE));
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Error saving create bucket operation in journal",e);
        }
    }

    public void deleteBucket(String bucketName){
        final boolean result;
        try {
            sqlService.getConnection();
            result = sqlService.insertJournal(new Journal(bucketName, Operation.DELETE));
            if(result)  
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Error saving delete bucket operation in journal",e);
        }
    }

    public void deletePurgeObject(String bucketName, String objKey, Operation operation){
        final boolean result;
        try {
            //getObject but searchInDeleted and searchInPurge
            RepoObject object = sqlService.getObject(bucketName, objKey, null, null, null, true, true);
            result = sqlService.insertJournal(new Journal(bucketName, objKey, operation, object.getVersionChecksum()));
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Error saving " + operation.getValue() + " object operation in journal",e);
        }
    }

    public void createUpdateObject(String bucketName, String objKey, Operation operation, String versionChecksum){
        final boolean result;
        try {
            result = sqlService.insertJournal(new Journal(bucketName, objKey, operation, versionChecksum));
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Error saving " + operation.getValue() + " object operation in journal",e);
        }
    }

    public void createUpdateCollection(String bucketName, String collKey, Operation operation, String versionChecksum, List<InputObject> objects){
        boolean result;
        try {
            result = sqlService.insertJournal(new Journal(bucketName, null, collKey, operation, null, versionChecksum));
            if(result) {
                for (InputObject inputObject : objects) {
                    Journal journal = new Journal(bucketName, inputObject.getKey(), collKey, Operation.CREATE, inputObject.getVersionChecksum(), versionChecksum);
                    if (!sqlService.insertJournal(journal)) {
                        result = false;
                        break;
                    }
                }
            }
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Error saving " + operation.getValue() + " collection operation in journal",e);
        }
    }

    public void deleteCollection(String bucketName, String collKey, String versionChecksum){
        final boolean result;
        try {
            if(versionChecksum == null) {
                RepoCollection collection = sqlService.getCollection(bucketName, collKey);
                if(collection != null)
                    versionChecksum = collection.getVersionChecksum();
            }
            result = sqlService.insertJournal(new Journal(bucketName, null, collKey, Operation.DELETE, null, versionChecksum));
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Error saving " + Operation.DELETE.getValue() + " collection operation in journal",e);
        }
    }
}
