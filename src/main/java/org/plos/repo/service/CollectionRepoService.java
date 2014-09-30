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
import org.plos.repo.models.*;
import org.plos.repo.models.Object;
import org.plos.repo.models.validator.InputCollectionValidator;
import org.plos.repo.util.VersionChecksumGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.sql.SQLException;
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

  @Inject
  private VersionChecksumGenerator versionChecksumGenerator;


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

      if (key == null)
        throw new RepoException(RepoException.Type.NoCollectionKeyEntered);

      if (elementFilter == null || elementFilter.isEmpty()) // no filters defined
        collection = sqlService.getCollection(bucketName, key);
      else
        collection = sqlService.getCollection(bucketName, key, elementFilter.getVersion(), elementFilter.getTag(), elementFilter.getVersionChecksum());

      if (collection == null)
        throw new RepoException(RepoException.Type.CollectionNotFound);

      collection.setVersions(this.getCollectionVersions(collection));

      return collection;

    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {
      sqlReleaseConnection();
    }

  }

  /**
   * Returns a list of all collection versions for the given <code>collection</code>
   * @param collection a single {@link org.plos.repo.models.Collection}
   * @return a list of {@link org.plos.repo.models.Collection}
   * @throws org.plos.repo.service.RepoException
   */
  public List<Collection> getCollectionVersions(Collection collection) throws RepoException {

    try {
      sqlService.getConnection();
      return sqlService.listCollectionVersions(collection);
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

      if (key == null)
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
   * @param inputCollection a {@link org.plos.repo.models.InputCollection} that holds the information of the new collection
   *                        to be created
   * @return {@link org.plos.repo.models.Collection} created
   * @throws RepoException
   */
  public Collection createCollection(CreateMethod method, InputCollection inputCollection) throws RepoException {

    inputCollectionValidator.validate(inputCollection);

    Collection existingCollection;


    // fetch the collection if it already exists
    try {
      existingCollection = getCollection(inputCollection.getBucketName(), inputCollection.getKey(), new ElementFilter()); // don't want to take in count the tag for getting the existing collection
    } catch (RepoException e) {
      if (e.getType() == RepoException.Type.CollectionNotFound)
        existingCollection = null;
      else
        throw e;
    }

    Timestamp creationDate = inputCollection.getCreationDateTime() != null ?
                Timestamp.valueOf(inputCollection.getCreationDateTime()) : new Timestamp(new Date().getTime());

    Timestamp timestamp = inputCollection.getTimestamp() != null ?
        Timestamp.valueOf(inputCollection.getTimestamp()) : creationDate;

    switch (method) {

      case NEW:
        if (existingCollection != null)
          throw new RepoException(RepoException.Type.CantCreateNewCollectionWithUsedKey);
        return createNewCollection(inputCollection.getKey(), inputCollection.getBucketName(), timestamp, inputCollection.getObjects(), inputCollection.getTag(), creationDate);

      case VERSION:
        if (existingCollection == null)
          throw new RepoException(RepoException.Type.CantCreateCollectionVersionWithNoOrig);
        return updateCollection(inputCollection.getKey(), inputCollection.getBucketName(), timestamp, existingCollection, inputCollection.getObjects(), inputCollection.getTag(), creationDate);

      case AUTO:
        if (existingCollection == null)
          return createNewCollection(inputCollection.getKey(), inputCollection.getBucketName(), timestamp, inputCollection.getObjects(), inputCollection.getTag(), creationDate);
        else
          return updateCollection(inputCollection.getKey(), inputCollection.getBucketName(), timestamp, existingCollection, inputCollection.getObjects(), inputCollection.getTag(), creationDate);

      default:
        throw new RepoException(RepoException.Type.InvalidCreationMethod);
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
      sqlService.getConnection();
      bucket = sqlService.getBucket(bucketName);
    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {
      sqlReleaseConnection();
    }

    if (bucket == null)
      throw new RepoException(RepoException.Type.BucketNotFound);

    return createCollection(key, bucketName, timestamp, bucket.bucketId, objects, tag, creationDate);
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

    return createCollection(key, bucketName, timestamp, existingCollection.getBucketId(), objects, tag, creationDate);

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
        if (object.key.equals(inputObject.getKey()) &&
            object.bucketName.equals(bucketName) &&
            object.checksum.equals(inputObject.getVersionChecksum())){
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
                                      Timestamp creationDate) throws RepoException {

    Integer versionNumber;
    Collection collection;
    boolean rollback = false;

    try {

      sqlService.getConnection();

      try {
        versionNumber = sqlService.getCollectionNextAvailableVersion(bucketName, key);   // change to support collections
      } catch (SQLException e) {
        throw new RepoException(e);
      }

      collection = new Collection(null, key, timestamp, bucketId, bucketName, versionNumber, Status.USED, tag, creationDate, null);

      List<Integer> objectsChecksum = Lists.newArrayList(Iterables.transform(inputObjects, typeFunction()));

      collection.setVersionChecksum(versionChecksumGenerator.generateVersionChecksum(collection, objectsChecksum));

      rollback = true;

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

      sqlService.transactionCommit();

      rollback = false;

      return getCollection(bucketName, key, new ElementFilter(versionNumber, tag, null));

    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {

      if (rollback) {
        sqlRollback("collection " + bucketName + ", " + key);
      }
      sqlReleaseConnection();
    }

  }

  private Function<InputObject, Integer> typeFunction() {
    return new Function<InputObject, Integer>() {

      @Override
      public Integer apply(InputObject inputObject) {
        return inputObject.getVersionChecksum();
      }

    };
  }

  @Override
  public Logger getLog() {
    return log;
  }
}
