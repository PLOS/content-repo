package org.plos.repo.service;



import org.plos.repo.models.Audit;
import org.plos.repo.models.Operation;
import org.plos.repo.models.RepoCollection;
import org.plos.repo.models.RepoObject;
import org.plos.repo.models.Status;

import java.sql.SQLException;

import javax.inject.Inject;
import org.plos.repo.models.input.ElementFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This service handles all communication for audit events with sqlservice
 */
public class AuditService {

    private static final Logger log = LoggerFactory.getLogger(AuditService.class);

    @Inject
    private SqlService sqlService;

    /**
     * Audit the create bucket operation
     * @param bucketName bucket's name audited
     */
    public void createBucket(String bucketName){
        final boolean result;
        try {
            sqlService.getConnection();
            result = sqlService.insertAudit(new Audit(bucketName, Operation.CREATE_BUCKET));
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Exception: {} when trying to save audit for create-bucket operation, bucketName {}",
                    e.getMessage(), bucketName);
        } finally {
            sqlReleaseConnection();
        }
    }

    /**
     * Audit the delete bucket operation
     * @param bucketName bucket's name audited
     */
    public void deleteBucket(String bucketName){
        final boolean result;
        try {
            sqlService.getConnection();
            result = sqlService.insertAudit(new Audit(bucketName, Operation.DELETE_BUCKET));
            if(result)  
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Exception: {} when trying to save audit for delete-bucket operation, bucketName {}",
                    e.getMessage(), bucketName);
        } finally {
            sqlReleaseConnection();
        }
    }

    /**
     * Audit the delete and purge object operations
     * @param object RepoObject audited
     */
    public void deletePurgeObject(RepoObject object){
        final boolean result;
        final Operation operation = Status.DELETED.equals(object.getStatus()) ? Operation.DELETE_OBJECT : Operation.PURGE_OBJECT;
        try {
            sqlService.getConnection();
            result = sqlService.insertAudit(new Audit(object.getBucketName(), object.getKey(), operation, object.getVersionChecksum()));
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Exception: {} when trying to save audit for {} object operation, bucketName {}, key{}, versionChecksum {}",
                    e.getMessage(), 
                    operation.getValue(), 
                    object.getBucketName(), 
                    object.getKey(), 
                    object.getVersionChecksum());
            
        } finally {
            sqlReleaseConnection();
        }
    }

    /**
     * Audit the create and update object operations
     * @param object RepoObject audited
     */
    public void createUpdateObject(RepoObject object){
        final boolean result;
        final Operation operation = object.getVersionNumber() > 0 ? Operation.UPDATE_OBJECT : Operation.CREATE_OBJECT;
        try {
            Audit audit = new Audit(object.getBucketName(), object.getKey(), operation, object.getVersionChecksum());;
            sqlService.getConnection();
            result = sqlService.insertAudit(audit);
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Exception: {} when trying to save audit for {} object operation, bucketName {}, key{}, versionChecksum {}",
                    e.getMessage(),
                    operation.getValue(),
                    object.getBucketName(),
                    object.getKey(),
                    object.getVersionChecksum());
        } finally {
            sqlReleaseConnection();
        }
    }

    /**
     * Audit the create and update collection operations
     * @param collection RepoCollection audited
     */
    public void createUpdateCollection(RepoCollection collection){
        final boolean result;
        final Operation operation = collection.getVersionNumber() > 0 ? Operation.UPDATE_COLLECTION : Operation.CREATE_COLLECTION;
        try {
            Audit audit = new Audit(collection.getBucketName(), collection.getKey(), operation, collection.getVersionChecksum());
            sqlService.getConnection();
            result = sqlService.insertAudit(audit);
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Exception: {} when trying to save audit for {} collection operation, bucketName {}, key{}, versionChecksum {}",
                    e.getMessage(),
                    operation.getValue(),
                    collection.getBucketName(),
                    collection.getKey(),
                    collection.getVersionChecksum());
        } finally {
            sqlReleaseConnection();
        }
    }

    /**
     * Audit the delete collection operation 
     * @param bucketName Bucket's name audited
     * @param collKey Key collection audited
     * @param elementFilter ElementFilter 
     */
    public void deleteCollection(String bucketName, String collKey, ElementFilter elementFilter){
        final boolean result;
        String versionChecksum = elementFilter.getVersionChecksum();
        try {
            sqlService.getConnection();
            if(versionChecksum == null) {
                RepoCollection collection = sqlService.getCollection(bucketName, collKey, elementFilter.getVersion(),
                        elementFilter.getVersionChecksum(), elementFilter.getTag(), true);
                if(collection != null) {
                    versionChecksum = collection.getVersionChecksum();
                }
            }
            result = sqlService.insertAudit(new Audit(bucketName, collKey, Operation.DELETE_COLLECTION, versionChecksum));
            if(result)
                sqlService.transactionCommit();
        } catch (SQLException e){
            log.error("Exception: {} when trying to save audit for delete-collection operation, bucketName {}, key{}, versionChecksum {}",
                    e.getMessage(),
                    bucketName,
                    collKey,
                    versionChecksum);
        } finally {
            sqlReleaseConnection();
        }
    }

    /**
     * Release connection
     */
    private void sqlReleaseConnection() {
        try {
            sqlService.releaseConnection();
        } catch (SQLException e) {
            log.error("Error release connection",e);
        }

    }
}
