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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.UUID;

public class FileSystemStoreService extends ObjectStore {

  private static final Logger log = LoggerFactory.getLogger(FileSystemStoreService.class);

  private String dataDirectory;

  private String reproxyBaseUrl;

  public FileSystemStoreService(String dataDirectory, String reproxyBaseUrl) {
    this.dataDirectory = dataDirectory;
    this.reproxyBaseUrl = reproxyBaseUrl;

    File dir = new File(dataDirectory);
    dir.mkdir();
  }

  private String getBucketLocationString(String bucketName) {
    return dataDirectory + "/" + bucketName + "/";
  }

  public String getObjectLocationString(String bucketName, String checksum) {
    return getBucketLocationString(bucketName) + checksum.substring(0, 2) + "/" + checksum;
  }

  @Override
  public boolean objectExists(RepoObject repoObject) {
    return new File(getObjectLocationString(repoObject.getBucketName(), repoObject.getChecksum())).exists();
  }

  @Override
  public InputStream getInputStream(RepoObject repoObject) {
    try {
      return new FileInputStream(getObjectLocationString(repoObject.getBucketName(), repoObject.getChecksum()));
    } catch (FileNotFoundException e) {
      log.debug("The content for the object was not found. Object --> key {} , bucket name: {} , content checksum: {} , version number: {} ",
          repoObject.getKey(),
          repoObject.getBucketName(),
          repoObject.getChecksum(),
          repoObject.getVersionNumber());
      return null;
    }
  }

  @Override
  public Optional<Boolean> bucketExists(Bucket bucket) {
    return Optional.of(new File(getBucketLocationString(bucket.getBucketName())).isDirectory());
  }

  @Override
  public Optional<Boolean> createBucket(Bucket bucket) {
    File dir = new File(getBucketLocationString(bucket.getBucketName()));
    boolean result = dir.mkdir();

    if (!result) {
      log.error("Error while creating bucket. Directory was not able to be created : " + getBucketLocationString(bucket.getBucketName()));
    }

    return Optional.of(result);
  }

  @Override
  public boolean hasXReproxy() {
    return reproxyBaseUrl != null;
  }

  @Override
  public String[] getFilePaths(RepoObject repoObject) throws RepoException {
    String path = null;
    try {
      if (!hasXReproxy()) {
        return new String[0]; // since the filesystem is not reproxyable
      }

      path = reproxyBaseUrl + "/" + repoObject.getBucketName() + "/" + repoObject.getChecksum().substring(0, 2) + "/" + repoObject.getChecksum();

      if (path == null) {
        throw new RepoException(RepoException.Type.ObjectFilePathMissing);
      }
    } catch (Exception e) {
      throw new RepoException(e);
    }
    return new String[]{path};
  }

  @Override
  public Optional<Boolean> deleteBucket(Bucket bucket) {
    File dir = new File(getBucketLocationString(bucket.getBucketName()));
    return Optional.of(dir.delete());
  }

  @Override
  public boolean saveUploadedObject(Bucket bucket, UploadInfo uploadInfo, RepoObject repoObject) {
    File tempFile = new File(uploadInfo.getTempLocation());

    File newFile = new File(getObjectLocationString(bucket.getBucketName(), uploadInfo.getChecksum()));

    // create the subdirectory if it does not exist
    File subDir = new File(newFile.getParent());

    if (!subDir.exists()) {
      if (!subDir.mkdir()) {
        log.error("Object subdirectory was not able to be created : " + subDir);
        return false;
      }
    }

    return tempFile.renameTo(newFile);
  }

  @Override
  public boolean deleteObject(RepoObject repoObject) {
    File file = new File(getObjectLocationString(repoObject.getBucketName(), repoObject.getChecksum()));
    File parentDir = new File(file.getParent());

    boolean result = file.delete();

    // delete the parent subdirectory if it is empty

    if (parentDir.isDirectory() && parentDir.list().length == 0) {
      parentDir.delete(); // TODO: log an error if this fails
    }

    return result;
  }

  @Override
  public boolean deleteTempUpload(UploadInfo uploadInfo) {
    return new File(uploadInfo.getTempLocation()).delete();
  }

  @Override
  public UploadInfo uploadTempObject(InputStream uploadedInputStream) throws RepoException {
    final String tempFileLocation = dataDirectory + "/" + UUID.randomUUID().toString() + ".tmp";

    try {
     if (uploadedInputStream == null) {
       throw new RepoException(RepoException.Type.NoFileEntered);
     }

      FileOutputStream fos = new FileOutputStream(tempFileLocation);

      ReadableByteChannel in = Channels.newChannel(uploadedInputStream);
      MessageDigest digest = checksumGenerator.getDigestMessage();
      WritableByteChannel out = Channels.newChannel(new DigestOutputStream(fos, digest));
      ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);

      long size = 0;

      while (in.read(buffer) != -1) {
        buffer.flip();
        size += out.write(buffer);
        buffer.clear();
      }

      fos.flush();

      final String checksum = checksumGenerator.checksumToString(digest.digest());
      final long finalSize = size;

      in.close();
      out.close();

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
