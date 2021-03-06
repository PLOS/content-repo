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
import com.guba.mogilefs.MogileFS;
import com.guba.mogilefs.PooledMogileFSImpl;
import org.apache.commons.io.FileUtils;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.RepoObject;
import org.plos.repo.util.ChecksumGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.UUID;

public class MogileStoreService extends ObjectStore {

  private static final Logger log = LoggerFactory.getLogger(MogileStoreService.class);

  public static final String mogileFileClass = "";

  public MogileFS mfs = null;

  public MogileStoreService(String domain, String[] trackerStrings, int maxTrackerConnections, int maxIdleConnections, long maxIdleTimeMillis) throws Exception {
    mfs = new PooledMogileFSImpl(domain, trackerStrings, maxTrackerConnections, maxIdleConnections, maxIdleTimeMillis);
  }

  private String getObjectLocationString(String bucketName, String checksum) {
    return checksum + "-" + bucketName;
  }

  @Override
  public boolean objectExists(RepoObject repoObject) {
    try {
      InputStream in = mfs.getFileStream(getObjectLocationString(repoObject.getBucketName(), repoObject.getChecksum()));

      if (in == null) {
        return false;
      }

      in.close();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public boolean hasXReproxy() {
    return true;
  }

  @Override
  public String[] getFilePaths(RepoObject repoObject) throws RepoException {
    String[] paths = null;
    try {
      paths = mfs.getPaths(getObjectLocationString(repoObject.getBucketName(), repoObject.getChecksum()), true);

      if (paths == null) {
        throw new RepoException(RepoException.Type.ObjectFilePathMissing);
      }
    } catch (Exception e) {
      throw new RepoException(e);
    }

    return paths;
  }

  @Override
  public InputStream getInputStream(RepoObject repoObject) throws RepoException {
    try {
      return mfs.getFileStream(getObjectLocationString(repoObject.getBucketName(), repoObject.getChecksum()));
    } catch (Exception e) {
      throw new RepoException(e);
    }
  }

  @Override
  public Optional<Boolean> bucketExists(Bucket bucket) {
    return Optional.absent();
  }

  @Override
  public Optional<Boolean> createBucket(Bucket bucket) {
    // we use file paths instead of domains so this function does not need to do anything
    return Optional.absent();
  }

  @Override
  public Optional<Boolean> deleteBucket(Bucket bucket) {
    return Optional.absent();
  }

  @Override
  public UploadInfo uploadTempObject(InputStream uploadedInputStream) throws RepoException {
    final String tempKey = UUID.randomUUID().toString() + ".tmp";
    File tempFile = null;
    try {
      tempFile = File.createTempFile("input", "mogile");
      MessageDigest digest = ChecksumGenerator.getDigestMessage();
      InputStream dis = new DigestInputStream(uploadedInputStream, digest);
      FileUtils.copyInputStreamToFile(dis, tempFile);
      mfs.storeFile(tempKey, mogileFileClass, tempFile);

      final String checksum = ChecksumGenerator.checksumToString(digest.digest());
      final long finalSize = tempFile.length();
      
      return new UploadInfo() {
        @Override
        public Long getSize() {
          return finalSize;
        }

        @Override
        public String getTempLocation() {
          return tempKey;
        }

        @Override
        public String getChecksum() {
          return checksum;
        }
      };
    } catch (Exception e) {
      throw new RepoException(e);
    } finally {
      if (tempFile != null) {
        tempFile.delete();
      }
    }
  }

  @Override
  public boolean saveUploadedObject(Bucket bucket, UploadInfo uploadInfo, RepoObject repoObject) {
    try {
      mfs.rename(uploadInfo.getTempLocation(), getObjectLocationString(bucket.getBucketName(), uploadInfo.getChecksum()));
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public boolean deleteTempUpload(UploadInfo uploadInfo) {
    try {
      mfs.delete(uploadInfo.getTempLocation());
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public boolean deleteObject(RepoObject repoObject) {
    try {
      mfs.delete(getObjectLocationString(repoObject.getBucketName(), repoObject.getChecksum()));
      return true;
    } catch (Exception e) {
      return false;
    }
  }

}
