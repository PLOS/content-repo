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

import com.google.common.base.Optional;
import org.apache.commons.io.IOUtils;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.RepoObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
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

  @Override
  public boolean objectExists(RepoObject repoObject) {
    return (data.get(repoObject.getBucketName()) != null && data.get(repoObject.getBucketName()).get(repoObject.getChecksum()) != null);
  }

  @Override
  public InputStream getInputStream(RepoObject repoObject) {
    Map<String, byte[]> bucket = data.get(repoObject.getBucketName());
    if (bucket != null) {
      byte[] content = bucket.get(repoObject.getChecksum());
      if (content != null) {
        return new ByteArrayInputStream(content);
      }
    }
    log.debug("The content for the object was not found. Object --> key {} , bucket name: {} , content checksum: {} , version number: {} ",
        repoObject.getKey(),
        repoObject.getBucketName(),
        repoObject.getChecksum(),
        repoObject.getVersionNumber());
    return null;
  }

  @Override
  public Optional<Boolean> bucketExists(Bucket bucket) {
    return Optional.of(data.containsKey(bucket.getBucketName()));
  }

  @Override
  public Optional<Boolean> createBucket(Bucket bucket) {
    return Optional.of(data.put(bucket.getBucketName(), new HashMap<String, byte[]>()) == null);
  }

  @Override
  public boolean hasXReproxy() {
    return false;
  }


  @Override
  public String[] getFilePaths(RepoObject repoObject) throws RepoException {
    return new String[0]; // since the filesystem is not reproxyable
  }

  @Override
  public Optional<Boolean> deleteBucket(Bucket bucket) {
    // TODO: what if it contains stuff?

    return Optional.of(data.remove(bucket.getBucketName()) != null);
  }

  @Override
  public boolean saveUploadedObject(Bucket bucket, UploadInfo uploadInfo, RepoObject repoObject) {
    byte[] tempContent = tempdata.get(uploadInfo.getTempLocation());
    data.get(bucket.getBucketName()).put(uploadInfo.getChecksum(), tempContent);
    return (tempdata.remove(uploadInfo.getTempLocation()) != null);
  }

  @Override
  public boolean deleteObject(RepoObject repoObject) {
    if (!objectExists(repoObject)) {
      return false;
    }

    return data.get(repoObject.getBucketName()).remove(repoObject.getChecksum()) != null;
  }

  @Override
  public boolean deleteTempUpload(UploadInfo uploadInfo) {
    tempdata.remove(uploadInfo.getTempLocation());

    return true;
  }

  @Override
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
