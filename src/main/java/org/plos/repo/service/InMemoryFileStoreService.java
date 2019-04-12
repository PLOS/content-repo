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
import org.apache.commons.io.IOUtils;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.RepoObject;
import org.plos.repo.util.ChecksumGenerator;
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
      MessageDigest digest = ChecksumGenerator.getDigestMessage();

      final String tempFileLocation = UUID.randomUUID().toString() + ".tmp";

      byte[] bytes = IOUtils.toByteArray(uploadedInputStream);

      tempdata.put(tempFileLocation, bytes);

      final String checksum = ChecksumGenerator.checksumToString(digest.digest(bytes));
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
