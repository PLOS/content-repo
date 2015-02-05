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

import org.plos.repo.models.Bucket;
import org.plos.repo.models.RepoObject;
import org.plos.repo.models.Status;
import org.plos.repo.util.ChecksumGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Pattern;

public abstract class ObjectStore {

  private static final Logger log = LoggerFactory.getLogger(MogileStoreService.class);
  
  @Inject
  protected ChecksumGenerator checksumGenerator;

  public static interface UploadInfo {
    Long getSize();
    String getTempLocation();
    String getChecksum();
  }

  public static boolean isValidFileName(String name) {
    return !Pattern.compile("[^-_.A-Za-z0-9]").matcher(name).find();
  }

//  public static boolean isValidBucketName(String bucketName) {
//    return BucketNameUtils.isValidV2BucketName(bucketName);
//  }
  /**
   * Retrieve the URLs of the given repo object <code>repoObject</code>. Return null if the
   * data does not exist and set the status MISSING_DATA to object, or throw a 
   * {@link org.plos.repo.service.RepoException} if an error occurs.
   *
   * @param repoObject a single {@link org.plos.repo.models.RepoObject} that represents the
   *                   object to be searched.
   * @return an URL array  wih the url of data of the given repoObject
   * @throws RepoException
   */
  protected URL[] getRedirectURLs(RepoObject repoObject) throws RepoException{
    URL[] urls = null;
    try {
      String[] paths = getFilePaths(repoObject);
      if( paths != null && paths.length > 0 ) {
        int pathCount = paths.length;
        urls = new URL[pathCount];

        for (int i = 0; i < pathCount; i++) {
          urls[i] = new URL(paths[i]);
        }
        
      } else { 
        repoObject.setStatus(Status.MISSING_DATA);
        log.info(" Missing Data when trying to fetch reproxy url, key: {} , bucket name: {} , content checksum: {} , version number: {} ",
                repoObject.getKey(),
                repoObject.getBucketName(),
                repoObject.getChecksum(),
                repoObject.getVersionNumber());
      }

    } catch (MalformedURLException e) {
      throw new RepoException(RepoException.Type.ServerError);
    } catch (Exception e) {
      throw new RepoException(e);
    }
    return urls;
    
  }

  abstract public Boolean hasXReproxy();

  abstract public String[]  getFilePaths(RepoObject repoObject) throws RepoException;

  abstract public Boolean objectExists(RepoObject repoObject);

  // we use Boolean here
  abstract public Boolean bucketExists(Bucket bucket);

  abstract public Boolean createBucket(Bucket bucket);

  abstract public Boolean deleteBucket(Bucket bucket);

  abstract public UploadInfo uploadTempObject(InputStream uploadedInputStream) throws RepoException;

  abstract public Boolean saveUploadedObject(Bucket bucket, UploadInfo uploadInfo, RepoObject repoObject);

  abstract public Boolean deleteObject(RepoObject repoObject);

  abstract public Boolean deleteTempUpload(UploadInfo uploadInfo);

  /**
   * Retrieve the data of the given repo object <code>repoObject</code>. Return null if the
   * data does not exist, or throw a {@link org.plos.repo.service.RepoException} if an error
   * occurs.
   *
   * @param repoObject a single {@link org.plos.repo.models.RepoObject} that represents the
   *                   object to be searched.
   * @return an inputStream object wih the data of the given repoObject
   * @throws RepoException
   */
  abstract public InputStream getInputStream(RepoObject repoObject) throws RepoException;

}
