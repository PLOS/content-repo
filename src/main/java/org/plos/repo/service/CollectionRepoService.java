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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.hsqldb.lib.StringUtil;
import org.plos.repo.models.*;
import org.plos.repo.models.Object;
import org.plos.repo.models.input.ElementFilter;
import org.plos.repo.models.input.InputCollection;
import org.plos.repo.models.input.InputObject;
import org.plos.repo.models.validator.InputCollectionValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

/**
 * This service handles all communication for collections with sqlservice
 */
public class CollectionRepoService extends BaseRepoService {

  private static final Logger log = LoggerFactory.getLogger(CollectionRepoService.class);

  @Inject
  private InputCollectionValidator inputCollectionValidator;


  /**
   * Returns a list of collections for the given bucket name <code>bucketName</code>. In case pagination
   * parameters <code>offset</code> and <code>limit</code> are not present, it loads the default pagination data.
   * @param bucketName a single String representing the bucket name in where to look the collection
   * @param offset an Integer used to determine the offset of the response
   * @param limit an Integer used to determine the limit of the response
   * @param includeDeleted a boolean value that defines whether to include deleted collections or not
   * @param tag a single String used to filter the response when collection's tag matches the given param
   * @return a list of {@link org.plos.repo.models.Collection}
   * @throws org.plos.repo.service.RepoException
   */
  public List<Collection> listCollections(String bucketName, Integer offset, Integer limit, Boolean includeDeleted, String tag) throws RepoException {

    if (offset == null)
      offset = 0;
    if (limit == null)
      limit = DEFAULT_PAGE_SIZE;

    try {

      validatePagination(offset, limit);

      sqlService.getConnection();

      if (StringUtil.isEmpty(bucketName)){
        throw new RepoException(RepoException.Type.NoBucketEntered);
      }

      if (bucketName != null && sqlService.getBucket(bucketName) == null)
        throw new RepoException(RepoException.Type.BucketNotFound);

      return sqlService.listCollections(bucketName, offset, limit, includeDeleted, tag);

    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {
      sqlReleaseConnection();
    }

  }

  /**
   * Returns a collection identified by <code>bucketName</code> and <code>key</code>. If no filer <code>collectionFilter</code> is specified,
   * it returns the latest version available. If only tag filter is specified, and there is more than one collection with that tag, it returns
   * the last one.
   * @param bucketName a single String representing the bucket name in where to look the collection
   * @param key a single String identifying the collection key
   * @param elementFilter a collection filter object used to uniquely identify the collection
   * @return a collection {@link org.plos.repo.models.Collection} or null is the desired collection does not exists
   * @throws RepoException
   */
  public Collection getCollection(String bucketName, String key, ElementFilter elementFilter) throws RepoException {

    Collection collection;

    try {
      sqlService.getConnection();

      if (StringUtil.isEmpty(key))
        throw new RepoException(RepoException.Type.NoCollectionKeyEntered);

      if (elementFilter == null || elementFilter.isEmpty()) // no filters defined
        collection = sqlService.getCollection(bucketName, key);
      else
        collection = sqlService.getCollection(bucketName, key, elementFilter.getVersion(), elementFilter.getTag(), elementFilter.getVersionChecksum());

      if (collection == null)
        throw new RepoException(RepoException.Type.CollectionNotFound);

    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {
      sqlReleaseConnection();
    }

    return collection;

  }

  /**
   * Returns a list of all collection versions for the given <code>collection</code>
   * @param collection a single {@link org.plos.repo.models.Collection}
   * @return a list of {@link org.plos.repo.models.Collection}
   * @throws org.plos.repo.service.RepoException
   */

  /**
   * Returns a list of all versions for the given <code>bucketName</code> and <code>key</code>
   * @param bucketName a single a single String identifying the bucket name where the collection is.
   * @param key a single String identifying the collection key
   * @return a list of {@link org.plos.repo.models.Collection}
   * @throws org.plos.repo.service.RepoException
   */
  public List<Collection> getCollectionVersions(String bucketName, String key) throws RepoException {

    try {
      sqlService.getConnection();

      if (StringUtil.isEmpty(bucketName)){
        throw new RepoException(RepoException.Type.NoBucketEntered);
      }

      if (StringUtil.isEmpty(key)){
        throw new RepoException(RepoException.Type.NoCollectionKeyEntered);
      }

      List<Collection> collections = sqlService.listCollectionVersions(bucketName, key);

      if (collections == null || collections.size() == 0){
        throw new RepoException(RepoException.Type.CollectionNotFound);
      }

      return collections;

    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {
      sqlReleaseConnection();
    }
  }

  /**
   * Deletes the collection define by <code>bucketName</code>, <code>key</code> and <code>elementFilter</code>. If
   * only the tag in element filter is specified, and there is more than one collection matching the filter, it throws an
   * error.
   * @param bucketName a single String identifying the bucket name where the collection is.
   * @param key a single String identifying the collection key
   * @param elementFilter a collection filter object used to uniquely identify the collection
   * @throws org.plos.repo.service.RepoException
   */
  public void deleteCollection(String bucketName, String key, ElementFilter elementFilter) throws RepoException {

    boolean rollback = false;

    try {

      sqlService.getConnection();

      if (StringUtil.isEmpty(key))
        throw new RepoException(RepoException.Type.NoCollectionKeyEntered);

      if (elementFilter == null || (elementFilter.isEmpty())){
        throw new RepoException(RepoException.Type.NoFilterEntered);
      }

      rollback = true;

      if (elementFilter.getTag() != null & elementFilter.getVersionChecksum() == null & elementFilter.getVersion() == null){
        if (sqlService.listCollections(bucketName, 0, 10, false, elementFilter.getTag()).size() > 1){
          throw new RepoException(RepoException.Type.MoreThanOneTaggedCollection);
        }
      }

      if (sqlService.markCollectionDeleted(key, bucketName, elementFilter.getVersion(), elementFilter.getTag(), elementFilter.getVersionChecksum()) == 0)
        throw new RepoException(RepoException.Type.CollectionNotFound);

      sqlService.transactionCommit();
      rollback = false;

    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {

      if (rollback) {
        sqlRollback("object " + bucketName + ", " + key + ", " + elementFilter.toString());
      }

      sqlReleaseConnection();

    }
  }

  /**
   * Creates a new collection. It decides if it creates a collection from scratch or a new version of an existing one, based
   * on <code>method</code> input value.
   * @param method a {@link org.plos.repo.service.BaseRepoService.CreateMethod}
   * @param inputCollection a {@link org.plos.repo.models.input.InputCollection} that holds the information of the new collection
   *                        to be created
   * @return {@link org.plos.repo.models.Collection} created
   * @throws RepoException
   */
  public Collection createCollection(CreateMethod method, InputCollection inputCollection) throws RepoException {

    inputCollectionValidator.validate(inputCollection);

    Collection existingCollection = null;
    boolean rollback = false;
    Collection newCollection = null;

    try {

      // get connection
      sqlService.getConnection();

      existingCollection = sqlService.getCollection(inputCollection.getBucketName(), inputCollection.getKey());

      // creates timestamps
      Timestamp creationDate = inputCollection.getCreationDateTime() != null ?
          Timestamp.valueOf(inputCollection.getCreationDateTime()) : new Timestamp(new Date().getTime());

      Timestamp timestamp = inputCollection.getTimestamp() != null ?
          Timestamp.valueOf(inputCollection.getTimestamp()) : creationDate;

      if (CreateMethod.NEW.equals(method)){

        if (existingCollection != null)
          throw new RepoException(RepoException.Type.CantCreateNewCollectionWithUsedKey);
        newCollection = createNewCollection(inputCollection.getKey(), inputCollection.getBucketName(), timestamp, inputCollection.getObjects(), inputCollection.getTag(), creationDate);

      } else if (CreateMethod.VERSION.equals(method)){

        if (existingCollection == null)
          throw new RepoException(RepoException.Type.CantCreateCollectionVersionWithNoOrig);
        newCollection = updateCollection(inputCollection.getKey(), inputCollection.getBucketName(), timestamp, existingCollection, inputCollection.getObjects(), inputCollection.getTag(), creationDate);

      } else if (CreateMethod.AUTO.equals(method)){

        if (existingCollection == null)
          newCollection = createNewCollection(inputCollection.getKey(), inputCollection.getBucketName(), timestamp, inputCollection.getObjects(), inputCollection.getTag(), creationDate);
        else
          newCollection = updateCollection(inputCollection.getKey(), inputCollection.getBucketName(), timestamp, existingCollection, inputCollection.getObjects(), inputCollection.getTag(), creationDate);

      } else if (CreateMethod.AUTO.equals(method)){
        throw new RepoException(RepoException.Type.InvalidCreationMethod);
      }

      sqlService.transactionCommit();
      rollback = false;

      return newCollection;

    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {

      if (rollback) {
        sqlRollback("collection " + inputCollection.getBucketName() + ", " + inputCollection.getKey());
      }
      sqlReleaseConnection();
    }

  }


  private Collection createNewCollection(String key,
                                         String bucketName,
                                         Timestamp timestamp,
                                         List<InputObject> objects,
                                         String tag,
                                         Timestamp creationDate) throws RepoException {
    Bucket bucket = null;

    try {
      bucket = sqlService.getBucket(bucketName);

      if (bucket == null)
        throw new RepoException(RepoException.Type.BucketNotFound);


      return createCollection(key, bucketName, timestamp, bucket.bucketId, objects, tag, creationDate);
    } catch(SQLIntegrityConstraintViolationException e){
      throw new RepoException(RepoException.Type.CantCreateNewCollectionWithUsedKey);
    } catch (SQLException e) {
      throw new RepoException(e);
    }


  }

  private Collection updateCollection(String key,
                                      String bucketName,
                                      Timestamp timestamp,
                                      Collection existingCollection,
                                      List<InputObject> objects,
                                      String tag,
                                      Timestamp creationDate) throws RepoException {

    if (areCollectionsSimilar(key, bucketName, objects, tag, existingCollection)){
      return existingCollection;
    }

    try{
      return createCollection(key, bucketName, timestamp, existingCollection.getBucketId(), objects, tag, creationDate);
    } catch(SQLIntegrityConstraintViolationException e){
      throw new RepoException(RepoException.Type.CantCreateCollectionVersionWithNoOrig);
    } catch(SQLException e){
      throw new RepoException(e);
    }

  }

  private Boolean areCollectionsSimilar(String key,
                                        String bucketName,
                                        List<InputObject> objects,
                                        String tag,
                                        Collection existingCollection
  ){

    Boolean similar = existingCollection.getKey().equals(key) &&
        existingCollection.getBucketName().equals(bucketName) &&
        existingCollection.getStatus().equals(Status.USED) &&
        objects.size() == existingCollection.getObjects().size();


    if ( similar &&  ( existingCollection.getTag() != null && tag != null) ) {
      similar = existingCollection.getTag().equals(tag);
    } else {
      similar = similar && !( (existingCollection.getTag() != null && tag == null) || (existingCollection.getTag() == null && tag !=null)) ;
    }

    int i = 0;

    for ( ; i <  objects.size() & similar ; i++){

      InputObject inputObject = objects.get(i);

      int y = 0;
      for( ; y < existingCollection.getObjects().size(); y++ ){
        Object object = existingCollection.getObjects().get(y);
        if (object.getKey().equals(inputObject.getKey()) &&
            object.getVersionChecksum().equals(inputObject.getVersionChecksum())){
          break;

        }
      }

      if ( y == existingCollection.getObjects().size()){
        similar = false;
      }
    }


    return similar;

  }

  private Collection createCollection(String key,
                                      String bucketName,
                                      Timestamp timestamp,
                                      Integer bucketId,
                                      List<InputObject> inputObjects,
                                      String tag,
                                      Timestamp creationDate) throws SQLException, RepoException {

    Integer versionNumber;
    Collection collection;


    versionNumber = sqlService.getCollectionNextAvailableVersion(bucketName, key);   // change to support collections

    collection = new Collection(null, key, timestamp, bucketId, bucketName, versionNumber, Status.USED, tag, creationDate, null);

    List<String> objectsChecksum = Lists.newArrayList(Iterables.transform(inputObjects, typeFunction()));

    collection.setVersionChecksum(checksumGenerator.generateVersionChecksum(collection, objectsChecksum));

    // add a record to the DB
    Integer collId = sqlService.insertCollection(collection);
    if (collId == -1) {
      throw new RepoException("Error saving content to database");
    }

    for (InputObject inputObject : inputObjects){

      if (sqlService.insertCollectionObjects(collId, inputObject.getKey(), bucketName, inputObject.getVersionChecksum()) == 0){
        throw new RepoException(RepoException.Type.ObjectCollectionNotFound);
      }

    }

    return collection;


  }

  private Function<InputObject, String> typeFunction() {
    return new Function<InputObject, String>() {

      @Override
      public String apply(InputObject inputObject) {
        return inputObject.getVersionChecksum();
      }

    };
  }

  @Override
  public Logger getLog() {
    return log;
  }
}
