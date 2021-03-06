/*
 * Copyright (c) 2017 Public Library of Science
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

package org.plos.repo.service;

import com.google.common.base.Optional;
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

  private static final Logger log = LoggerFactory.getLogger(ObjectStore.class);

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
   * Retrieve the URLs of the given repo object <code>repoObject</code>. Set the status MISSING_DATA to object if the
   * file paths are null or occurs an error trying to get them, throw a {@link org.plos.repo.service.RepoException} if
   * an error occurs.
   *
   * @param repoObject a single {@link org.plos.repo.models.RepoObject} that represents the object to be searched.
   * @return an URL array  wih the url of data of the given repoObject
   * @throws RepoException
   */
  protected URL[] getRedirectURLs(RepoObject repoObject) throws RepoException {
    URL[] urls = new URL[0];
    try {
      String[] paths = getFilePaths(repoObject);

      if (paths.length > 0) {
        int pathCount = paths.length;
        urls = new URL[pathCount];

        for (int i = 0; i < pathCount; i++) {
          urls[i] = new URL(paths[i]);
        }
      }
    } catch (RepoException e) {
      if (RepoException.Type.ObjectFilePathMissing.equals(e.getType())) {
        repoObject.setStatus(Status.MISSING_DATA);
        log.error(" Missing Data when trying to fetch reproxy url, key: {} , bucket name: {} , content checksum: {} , version number: {} ",
            repoObject.getKey(),
            repoObject.getBucketName(),
            repoObject.getChecksum(),
            repoObject.getVersionNumber());
      } else {
        throw e;
      }
    } catch (MalformedURLException e) {
      throw new RepoException(RepoException.Type.ServerError);
    } catch (Exception e) {
      throw new RepoException(e);
    }
    return urls;
  }

  abstract public boolean hasXReproxy();

  /**
   * Retrieve the file paths of the given repo object <code>repoObject</code>. Throw a {@link
   * org.plos.repo.service.RepoException} if the path is null or an error occurs.
   *
   * @param repoObject a single {@link org.plos.repo.models.RepoObject} that represents the object to be searched.
   * @return an String array wih the file paths of the given repoObject
   * @throws RepoException
   */
  abstract public String[] getFilePaths(RepoObject repoObject) throws RepoException;

  abstract public boolean objectExists(RepoObject repoObject);

  /**
   * @return true if the bucket exists; false if it does not exist; absent if the implementation does not persist
   * buckets
   */
  abstract public Optional<Boolean> bucketExists(Bucket bucket);

  /**
   * @return true if the bucket was created; false if it could not be created; absent if this was a no-op because the
   * implementation does not persist buckets
   */
  abstract public Optional<Boolean> createBucket(Bucket bucket);

  /**
   * @return true if the bucket existed and was deleted; false if it did not exist or could not be deleted; absent if
   * this was a no-op because the implementation does not persist buckets
   */
  abstract public Optional<Boolean> deleteBucket(Bucket bucket);

  abstract public UploadInfo uploadTempObject(InputStream uploadedInputStream) throws RepoException;

  abstract public boolean saveUploadedObject(Bucket bucket, UploadInfo uploadInfo, RepoObject repoObject);

  abstract public boolean deleteObject(RepoObject repoObject);

  abstract public boolean deleteTempUpload(UploadInfo uploadInfo);

  /**
   * Retrieve the data of the given repo object <code>repoObject</code>. Return null if the data does not exist, or
   * throw a {@link org.plos.repo.service.RepoException} if an error occurs.
   *
   * @param repoObject a single {@link org.plos.repo.models.RepoObject} that represents the object to be searched.
   * @return an inputStream object wih the data of the given repoObject
   * @throws RepoException
   */
  abstract public InputStream getInputStream(RepoObject repoObject) throws RepoException;

}
