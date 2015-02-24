package org.plos.repo.service;



import org.plos.repo.models.*;

import java.sql.SQLException;
import java.util.List;

import javax.inject.Inject;
import org.plos.repo.models.input.ElementFilter;
import org.plos.repo.models.input.InputCollection;
import org.plos.repo.models.input.InputObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JournalService {

    private static final Logger log = LoggerFactory.getLogger(JournalService.class);

    @Inject
    private SqlService sqlService;
    
    
    public void createBucket(String bucketName){
        final boolean result;
        try {
            sqlService.getConnection();
            result = sqlService.insertJournal(new Journal(bucketName, Operation.CREATE_BUCKET));
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Error saving create bucket operation",e);
        } finally {
            sqlReleaseConnection();
        }
    }

    public void deleteBucket(String bucketName){
        final boolean result;
        try {
            sqlService.getConnection();
            result = sqlService.insertJournal(new Journal(bucketName, Operation.DELETE_BUCKET));
            if(result)  
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Error saving delete bucket operation",e);
        } finally {
            sqlReleaseConnection();
        }
    }

    public void deletePurgeObject(RepoObject object){
        final boolean result;
        final Operation operation = Status.DELETED.equals(object.getStatus()) ? Operation.DELETE_OBJECT : Operation.PURGE_OBJECT;
        try {
            /*String versionChecksum = elementFilter.getVersionChecksum();
            if(versionChecksum == null){
                versionChecksum = getObjectVersionChecksum(bucketName, objKey, elementFilter);
            }*/
            sqlService.getConnection();
            result = sqlService.insertJournal(new Journal(object.getBucketName(), object.getKey(), operation, object.getVersionChecksum()));
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Error saving " + operation.getValue() + " object operation",e);
        } finally {
            sqlReleaseConnection();
        }
    }

    public void createUpdateObject(RepoObject object){
        final boolean result;
        final Operation operation = object.getVersionNumber() > 0 ? Operation.UPDATE_OBJECT : Operation.CREATE_OBJECT;
        try {
            Journal journal = new Journal(object.getBucketName(), object.getKey(), operation, object.getVersionChecksum());;
            sqlService.getConnection();
            result = sqlService.insertJournal(journal);
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Error saving " + operation.getValue() + " operation",e);
        } finally {
            sqlReleaseConnection();
        }
    }

    public void createUpdateCollection(RepoCollection collection){
        final boolean result;
        final Operation operation = collection.getVersionNumber() > 0 ? Operation.UPDATE_COLLECTION : Operation.CREATE_COLLECTION;
        try {
            Journal journal = new Journal(collection.getBucketName(), collection.getKey(), operation, collection.getVersionChecksum());
            sqlService.getConnection();
            result = sqlService.insertJournal(journal);
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Error saving " + operation.getValue() + " operation",e);
        } finally {
            sqlReleaseConnection();
        }
    }

    public void deleteCollection(String bucketName, String collKey, ElementFilter elementFilter){
        final boolean result;
        try {
            String versionChecksum = elementFilter.getVersionChecksum();
            sqlService.getConnection();
            if(versionChecksum == null) {
                RepoCollection collection = sqlService.getCollection(bucketName, collKey, elementFilter.getVersion(),
                        elementFilter.getVersionChecksum(), elementFilter.getTag(), true);
                if(collection != null) {
                    versionChecksum = collection.getVersionChecksum();
                }
            }
            result = sqlService.insertJournal(new Journal(bucketName, collKey,Operation.DELETE_COLLECTION, versionChecksum));
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Error saving delete collection operation",e);
        } finally {
            sqlReleaseConnection();
        }
    }
    
    private void sqlReleaseConnection() {
        try {
            sqlService.releaseConnection();
        } catch (SQLException e) {
            log.error("Error release connection",e);
        }

    }
}
