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

import org.apache.commons.io.IOUtils;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.RepoObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryFileStoreService extends ObjectStore {

  private static final Logger log = LoggerFactory.getLogger(InMemoryFileStoreService.class);

  // bucketName -> data checksum -> file content
  private Map<String, Map<String, byte[]>> data = new ConcurrentHashMap<>();

  private Map<String, byte[]> tempdata = new ConcurrentHashMap<>();

  public InMemoryFileStoreService() {
  }

  public Boolean objectExists(RepoObject repoObject) {
    return (data.get(repoObject.getBucketName()) != null && data.get(repoObject.getBucketName()).get(repoObject.getChecksum()) != null);
  }

  public InputStream getInputStream(RepoObject repoObject) {
    byte[] content = data.get(repoObject.getBucketName()).get(repoObject.getChecksum());
    if (content != null) {
      return new ByteArrayInputStream(data.get(repoObject.getBucketName()).get(repoObject.getChecksum()));
    }
    log.debug("The content for the object was not found. Object --> key {} , bucket name: {} , content checksum: {} , version number: {} ",
        repoObject.getKey(),
        repoObject.getBucketName(),
        repoObject.getChecksum(),
        repoObject.getVersionNumber());
    return null;
  }

  public Boolean bucketExists(Bucket bucket) {
    return (data.containsKey(bucket.getBucketName()));
  }

  public Boolean createBucket(Bucket bucket) {
    return (data.put(bucket.getBucketName(), new HashMap<String, byte[]>()) == null);
  }

  public Boolean hasXReproxy() {
    return false;
  }

  public URL[] getRedirectURLs(RepoObject repoObject) {
    return new URL[]{}; // since the filesystem is not reproxyable
  }

  public Boolean deleteBucket(Bucket bucket) {

    // TODO: what if it contains stuff?

    return (data.remove(bucket.getBucketName()) != null);
  }

  public Boolean saveUploadedObject(Bucket bucket, UploadInfo uploadInfo, RepoObject repoObject) {

    byte[] tempContent = tempdata.get(uploadInfo.getTempLocation());
    data.get(bucket.getBucketName()).put(uploadInfo.getChecksum(), tempContent);
    return (tempdata.remove(uploadInfo.getTempLocation()) != null);

  }

  public Boolean deleteObject(RepoObject repoObject) {

    if (!objectExists(repoObject))
      return false;

    return data.get(repoObject.getBucketName()).remove(repoObject.getChecksum()) != null;

  }

  public Boolean deleteTempUpload(UploadInfo uploadInfo) {
    tempdata.remove(uploadInfo.getTempLocation());

    return true;
  }


  public UploadInfo uploadTempObject(InputStream uploadedInputStream) throws RepoException {

    try {
      MessageDigest digest = checksumGenerator.getDigestMessage();

      final String tempFileLocation = UUID.randomUUID().toString() + ".tmp";

      byte[] bytes = IOUtils.toByteArray(uploadedInputStream);

      tempdata.put(tempFileLocation, bytes);

      final String checksum = checksumGenerator.checksumToString(digest.digest(bytes));
      final long finalSize = bytes.length;

      return new UploadInfo() {
        @Override
        public Long getSize() {
          return finalSize;
        }

        @Override
        public String getTempLocation() {
          return tempFileLocation;
        }

        @Override
        public String getChecksum() {
          return checksum;
        }
      };
    } catch (Exception e) {
      throw new RepoException(e);
    }

  }

}
