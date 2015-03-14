package org.plos.repo.service;


import org.plos.repo.models.*;
import org.plos.repo.models.input.ElementFilter;
import org.plos.repo.util.UUIDFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.UUID;

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
            sqlService.getConnection();
            result = sqlService.insertJournal(new Journal(object.getBucketName(), object.getKey(), operation, object.getUuid()));
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
            Journal journal = new Journal(object.getBucketName(), object.getKey(), operation, object.getUuid());;
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
            Journal journal = new Journal(collection.getBucketName(), collection.getKey(), operation, collection.getUuid());
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

    public void deleteCollection(String bucketName, String collKey, ElementFilter elementFilter) throws RepoException {
        final boolean result;
        try {
            UUID uuid = UUIDFormatter.getUuid(elementFilter.getUuid());
            sqlService.getConnection();
            if(uuid == null) {
                RepoCollection collection = sqlService.getCollection(bucketName, collKey, elementFilter.getVersion(),
                    elementFilter.getTag(), uuid, true);
                if(collection != null) {
                  uuid = collection.getUuid();
                }
            }
            result = sqlService.insertJournal(new Journal(bucketName, collKey,Operation.DELETE_COLLECTION, uuid));
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
