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

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.RepoObject;
import org.plos.repo.util.ChecksumGenerator;

public class S3StoreService extends ObjectStore {

  private static final Logger log = LogManager.getLogger(S3StoreService.class);

  private AmazonS3Client s3Client = null;

  private static final String temp_upload_dir = "/tmp";

  // NOTE: our object versions does not make use of S3's versioning system

  public S3StoreService(String aws_access_key, String aws_secret_key) {
    s3Client = new AmazonS3Client(new BasicAWSCredentials(aws_access_key, aws_secret_key));
  }

  @Override
  public boolean objectExists(RepoObject repoObject) {
    try {
      S3Object obj = s3Client.getObject(repoObject.getBucketName(), repoObject.getChecksum());

      if (obj == null) {
        return false;
      }

      obj.close();
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
    String s3Url = s3Client.getResourceUrl(repoObject.getBucketName(), repoObject.getChecksum());

    if (s3Url == null) {
      throw new RepoException(RepoException.Type.ObjectFilePathMissing);
    }

    return new String[]{s3Url};
  }

  @Override
  public InputStream getInputStream(RepoObject repoObject) throws RepoException {
    try {
      return s3Client.getObject(repoObject.getBucketName(), repoObject.getChecksum()).getObjectContent();
    } catch (AmazonClientException e) {
      throw new RepoException(e);
    }
  }

  @Override
  public Optional<Boolean> bucketExists(Bucket bucket) {
    return Optional.of(s3Client.doesBucketExist(bucket.getBucketName()));
  }

  private static final Optional<Boolean> TRUE = Optional.of(true);
  private static final Optional<Boolean> FALSE = Optional.of(false);

  @Override
  public Optional<Boolean> createBucket(Bucket bucket) {
    try {
      CreateBucketRequest bucketRequest = new CreateBucketRequest(bucket.getBucketName(), Region.US_West);
      bucketRequest.withCannedAcl(CannedAccessControlList.PublicRead);
      s3Client.createBucket(bucketRequest);

      return TRUE;
    } catch (Exception e) {
      log.error("Error creating bucket", e);
      return FALSE;
    }
  }

  @Override
  public Optional<Boolean> deleteBucket(Bucket bucket) {
    try {
      s3Client.deleteBucket(bucket.getBucketName());
      return TRUE;
    } catch (Exception e) {
      log.error("Error deleting bucket", e);
      return FALSE;
    }
  }

  /**
   * Upload the file to the local server, calculate the checksum. Dont put in on S3 yet.
   *
   * @param uploadedInputStream
   * @return
   * @throws Exception
   */
  @Override
  public UploadInfo uploadTempObject(InputStream uploadedInputStream) throws RepoException {
    final String tempFileLocation = temp_upload_dir + "/" + UUID.randomUUID().toString() + ".tmp";

    try {
      FileOutputStream fos = new FileOutputStream(tempFileLocation);

      ReadableByteChannel in = Channels.newChannel(uploadedInputStream);
      MessageDigest digest = ChecksumGenerator.getDigestMessage();
      WritableByteChannel out = Channels.newChannel(new DigestOutputStream(fos, digest));
      ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);

      long size = 0;

      while (in.read(buffer) != -1) {
        buffer.flip();
        size += out.write(buffer);
        buffer.clear();
      }

      final String checksum = ChecksumGenerator.checksumToString(digest.digest());
      final long finalSize = size;

      out.close();
      in.close();

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

  @Override
  public boolean saveUploadedObject(Bucket bucket, UploadInfo uploadInfo, RepoObject repoObject) {
    int retries = 5;
    int tryCount = 0;
    int waitSecond = 4;

    ObjectMapper m = new ObjectMapper();
    Map<String, java.lang.Object> propsObj = m.convertValue(repoObject, Map.class);

    Map<String, String> propsStr = new HashMap<>();

    for (Map.Entry<String, java.lang.Object> entry : propsObj.entrySet()) {
      try {
        if (entry.getValue() == null) {
          propsStr.put(entry.getKey(), "");
        } else {
          propsStr.put(entry.getKey(), entry.getValue().toString());
        }
      } catch (ClassCastException cce) {
        log.error("Problem converting object to metadata", cce);
      }
    }

    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(uploadInfo.getSize());
    objectMetadata.setUserMetadata(propsStr);

    File tempFile = new File(uploadInfo.getTempLocation());

    PutObjectRequest putObjectRequest = new PutObjectRequest(bucket.getBucketName(), uploadInfo.getChecksum(), tempFile);
    putObjectRequest.withCannedAcl(CannedAccessControlList.PublicRead);
    putObjectRequest.setMetadata(objectMetadata);

    while (tryCount < retries) {
      try {
        s3Client.putObject(putObjectRequest); // TODO: check result and do something about it
        tempFile.delete();
        return true;
      } catch (Exception e) {
        tryCount++;

        log.error("Error during putObject", e);

        try {
          Thread.sleep(waitSecond * 1000);
        } catch (Exception e2) {
        }
      }
    }

    return false;
  }

  @Override
  public boolean deleteTempUpload(UploadInfo uploadInfo) {
    return new File(uploadInfo.getTempLocation()).delete();
  }

  @Override
  public boolean deleteObject(RepoObject repoObject) {
    try {
      s3Client.deleteObject(repoObject.getBucketName(), repoObject.getChecksum());
      return true;
    } catch (Exception e) {
      log.error("Error deleting object", e);
      return false;
    }
  }

}
