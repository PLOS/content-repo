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
import org.plos.repo.models.Object;
import org.plos.repo.util.ChecksumGenerator;

import javax.inject.Inject;
import java.io.InputStream;
import java.net.URL;
import java.util.regex.Pattern;

public abstract class ObjectStore {

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

  abstract public Boolean hasXReproxy();

  abstract public URL[] getRedirectURLs(org.plos.repo.models.Object object) throws RepoException;

  abstract public Boolean objectExists(Object object);

  // we use Boolean here
  abstract public Boolean bucketExists(Bucket bucket);

  abstract public Boolean createBucket(Bucket bucket);

  abstract public Boolean deleteBucket(Bucket bucket);

  abstract public UploadInfo uploadTempObject(InputStream uploadedInputStream) throws RepoException;

  abstract public Boolean saveUploadedObject(Bucket bucket, UploadInfo uploadInfo, Object object);

  abstract public Boolean deleteObject(Object object);

  abstract public Boolean deleteTempUpload(UploadInfo uploadInfo);

  abstract public InputStream getInputStream(Object object) throws RepoException;

}
