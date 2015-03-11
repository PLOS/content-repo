/*
 * Copyright (c) 2006-2014 by Public Library of Science
 * http://plos.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
 * This service audit the CREATE, UPDATE, DELETE and PURGE operations that are happening in the other services.
 */
public class AuditService extends BaseRepoService{

    private static final Logger log = LoggerFactory.getLogger(AuditService.class);

    
    /**
     * Audit the create bucket operation
     * @param bucketName bucket's name audited
     */
    public void createBucket(String bucketName){
        final boolean result;
        try {
            sqlService.getConnection();

            result = sqlService.insertAudit(new Audit(bucketName, Operation.CREATE_BUCKET));

            if(result) {
                sqlService.transactionCommit();
            }
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

            if(result) {
                sqlService.transactionCommit();
            }

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

            if(result) {
                sqlService.transactionCommit();
            }

        } catch (SQLException e){
            log.error("Exception: {} when trying to save audit for {} operation, bucketName {}, key{}, versionChecksum {}",
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
     * Audit the create object operation
     * @param object RepoObject audited
     */
    public void createObject(RepoObject object){
        final boolean result;
        try {
            Audit audit = new Audit(object.getBucketName(), object.getKey(), Operation.CREATE_OBJECT, object.getVersionChecksum());;

            sqlService.getConnection();

            result = sqlService.insertAudit(audit);

            if(result) {
                sqlService.transactionCommit();
            }

        } catch (SQLException e){
            log.error("Exception: {} when trying to save audit for create-object operation, bucketName {}, key{}, versionChecksum {}",
                    e.getMessage(),
                    object.getBucketName(),
                    object.getKey(),
                    object.getVersionChecksum());
        } finally {
            sqlReleaseConnection();
        }
    }

    /**
     * Audit the update object operation
     * @param object RepoObject audited
     */
    public void updateObject(RepoObject object){
        final boolean result;
        try {
            Audit audit = new Audit(object.getBucketName(), object.getKey(), Operation.UPDATE_OBJECT, object.getVersionChecksum());;

            sqlService.getConnection();

            result = sqlService.insertAudit(audit);

            if(result) {
                sqlService.transactionCommit();
            }

        } catch (SQLException e){
            log.error("Exception: {} when trying to save audit for update-object operation, bucketName {}, key{}, versionChecksum {}",
                e.getMessage(),
                object.getBucketName(),
                object.getKey(),
                object.getVersionChecksum());
        } finally {
            sqlReleaseConnection();
        }
    }

    /**
     * Audit the create collection operation
     * @param collection RepoCollection audited
     */
    public void createCollection(RepoCollection collection){
        final boolean result;
        try {
            Audit audit = new Audit(collection.getBucketName(), collection.getKey(), Operation.CREATE_COLLECTION, collection.getVersionChecksum());

            sqlService.getConnection();

            result = sqlService.insertAudit(audit);

            if(result) {
                sqlService.transactionCommit();
            }
        } catch (SQLException e){
            log.error("Exception: {} when trying to save audit for create-collection operation, bucketName {}, key{}, versionChecksum {}",
                    e.getMessage(),
                    
                    collection.getBucketName(),
                    collection.getKey(),
                    collection.getVersionChecksum());
        } finally {
            sqlReleaseConnection();
        }
    }

    /**
     * Audit the create collection operation
     * @param collection RepoCollection audited
     */
    public void updateCollection(RepoCollection collection){
        final boolean result;
        try {
            Audit audit = new Audit(collection.getBucketName(), collection.getKey(), Operation.UPDATE_COLLECTION, collection.getVersionChecksum());

            sqlService.getConnection();

            result = sqlService.insertAudit(audit);

            if(result) {
                sqlService.transactionCommit();
            }
        } catch (SQLException e){
            log.error("Exception: {} when trying to save audit for update-collection operation, bucketName {}, key{}, versionChecksum {}",
                e.getMessage(),
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

            if(result) {
                sqlService.transactionCommit();
            }
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

    @Override
    protected void sqlReleaseConnection (){
        try {
            super.sqlReleaseConnection();    
        } catch(RepoException e){
            log.error("Exception when trying to release connection.", e);
            
        }
    }

    @Override
    public Logger getLog() {
        return log;
    }
}
