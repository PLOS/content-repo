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

    public void deletePurgeObject(String bucketName, String objKey, Status status, ElementFilter elementFilter){
        final boolean result;
        final Operation operation = Status.DELETED.equals(status) ? Operation.DELETE_OBJECT : Operation.PURGE_OBJECT;
        try {
            String versionChecksum = elementFilter.getVersionChecksum();
            if(versionChecksum == null){
                versionChecksum = getObjectVersionChecksum(bucketName, objKey, elementFilter);
            }
            sqlService.getConnection();
            result = sqlService.insertJournal(new Journal(bucketName, objKey, operation, versionChecksum));
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Error saving " + operation.getValue() + " object operation",e);
        } finally {
            sqlReleaseConnection();
        }
    }

    public void createObject(String bucketName, String objKey, String versionChecksum){
        final boolean result;
        try {
            sqlService.getConnection();
            result = sqlService.insertJournal(new Journal(bucketName, objKey, Operation.CREATE_OBJECT, versionChecksum));
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Error saving create object operation",e);
        } finally {
            sqlReleaseConnection();
        }
    }

    public void updateObject(String bucketName, String objKey, String versionChecksum){
        final boolean result;
        try {
            sqlService.getConnection();
            result = sqlService.insertJournal(new Journal(bucketName, objKey, Operation.UPDATE_OBJECT, versionChecksum));
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Error saving update object operation",e);
        } finally {
            sqlReleaseConnection();
        }
    }

    public void createCollection(String bucketName, String collKey, String versionChecksum){
        boolean result;
        try {
            sqlService.getConnection();
            result = sqlService.insertJournal(new Journal(bucketName, collKey, Operation.CREATE_COLLECTION, versionChecksum));
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Error saving create collection operation",e);
        } finally {
            sqlReleaseConnection();
        }
    }

    public void updateCollection(String bucketName, String collKey, String versionChecksum){
        boolean result;
        try {
            sqlService.getConnection();
            result = sqlService.insertJournal(new Journal(bucketName, collKey, Operation.UPDATE_COLLECTION, versionChecksum));
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Error saving update collection operation",e);
        } finally {
            sqlReleaseConnection();
        }
    }

    public void deleteCollection(String bucketName, String collKey, ElementFilter elementFilter){
        final boolean result;
        try {
            String versionChecksum = elementFilter.getVersionChecksum();
            if(versionChecksum == null) {
                versionChecksum = getCollectionVersionChecksum(bucketName, collKey, elementFilter);
            }
            sqlService.getConnection();
            result = sqlService.insertJournal(new Journal(bucketName, collKey,Operation.DELETE_COLLECTION, versionChecksum));
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Error saving delete collection operation",e);
        } finally {
            sqlReleaseConnection();
        }
    }

    private String getObjectVersionChecksum(String bucketName, String objKey, ElementFilter elementFilter) throws SQLException{
        String versionChecksum = null;
        try {
            sqlService.getReadOnlyConnection();
            RepoObject object = sqlService.getObject(bucketName, objKey, elementFilter.getVersion(),
                    elementFilter.getVersionChecksum(), elementFilter.getTag(), true, true);
            if(object != null)
                versionChecksum = object.getVersionChecksum();
        } finally {
            sqlReleaseConnection();
        }
        return versionChecksum;
    }
    
    private String getCollectionVersionChecksum(String bucketName, String collKey, ElementFilter elementFilter) throws SQLException{
        String versionChecksum = null;
        try {
            sqlService.getReadOnlyConnection();
            RepoCollection collection = sqlService.getCollection(bucketName, collKey, elementFilter.getVersion(),
                    elementFilter.getVersionChecksum(), elementFilter.getTag(), true);
            if (collection != null)
                versionChecksum = collection.getVersionChecksum();
        } finally {
            sqlReleaseConnection();
        }
        return versionChecksum;
    }
    
    private void sqlReleaseConnection() {
        try {
            sqlService.releaseConnection();
        } catch (SQLException e) {
            log.error("Error release collection",e);
        }

    }
}
