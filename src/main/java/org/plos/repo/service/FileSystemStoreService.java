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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
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

  public Boolean objectExists(RepoObject repoObject) {
    return new File(getObjectLocationString(repoObject.getBucketName(), repoObject.getChecksum())).exists();
  }

  public InputStream getInputStream(RepoObject repoObject) throws RepoException {
    try {
      return new FileInputStream(getObjectLocationString(repoObject.getBucketName(), repoObject.getChecksum()));
    } catch (FileNotFoundException e) {
      throw new RepoException(e);
    }
  }

  public Boolean bucketExists(Bucket bucket) {
    return (new File(getBucketLocationString(bucket.getBucketName())).isDirectory());
  }

  public Boolean createBucket(Bucket bucket) {

    File dir = new File(getBucketLocationString(bucket.getBucketName()));
    boolean result = dir.mkdir();

    if (!result)
      log.error("Error while creating bucket. Directory was not able to be created : " + getBucketLocationString(bucket.getBucketName()));

    return result;
  }

  public Boolean hasXReproxy() {
    return reproxyBaseUrl != null;
  }

  public URL[] getRedirectURLs(RepoObject repoObject) throws RepoException {

    if (!hasXReproxy())
      return new URL[]{}; // since the filesystem is not reproxyable

    try {

      URL[] urls = new URL[1];
      urls[0] = new URL(reproxyBaseUrl + "/" + repoObject.getBucketName() + "/" + repoObject.getChecksum().substring(0, 2) + "/" + repoObject.getChecksum());
      return urls;

    } catch (MalformedURLException e) {
      throw new RepoException(RepoException.Type.ServerError);
    }

  }

  public Boolean deleteBucket(Bucket bucket) {
    File dir = new File(getBucketLocationString(bucket.getBucketName()));
    return dir.delete();
  }

  public Boolean saveUploadedObject(Bucket bucket, UploadInfo uploadInfo, RepoObject repoObject) {
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

  public Boolean deleteObject(RepoObject repoObject) {

    File file = new File(getObjectLocationString(repoObject.getBucketName(), repoObject.getChecksum()));
    File parentDir = new File(file.getParent());

    boolean result = file.delete();

    // delete the parent subdirectory if it is empty

    if (parentDir.isDirectory() && parentDir.list().length == 0)
      parentDir.delete(); // TODO: log an error if this fails

    return result;
  }

  public Boolean deleteTempUpload(UploadInfo uploadInfo) {
    return new File(uploadInfo.getTempLocation()).delete();
  }


  public UploadInfo uploadTempObject(InputStream uploadedInputStream) throws RepoException {
    final String tempFileLocation = dataDirectory + "/" + UUID.randomUUID().toString() + ".tmp";

    try {
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
