package org.plos.repo.service;

import org.apache.commons.io.IOUtils;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.Object;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryFileStoreService extends ObjectStore {

  // bucketName -> data checksum -> file content
  private Map<String, Map<String, byte[]>> data = new ConcurrentHashMap<>();

  private Map<String, byte[]> tempdata = new ConcurrentHashMap<>();

  public InMemoryFileStoreService() {
  }

  public Boolean objectExists(Object object) {
    return (data.get(object.getBucketName()) != null && data.get(object.getBucketName()).get(object.getChecksum()) != null);
  }

  public InputStream getInputStream(Object object) {
    return new ByteArrayInputStream(data.get(object.getBucketName()).get(object.getChecksum()));
  }

  public Boolean bucketExists(Bucket bucket) {
    return (data.containsKey(bucket.bucketName));
  }

  public Boolean createBucket(Bucket bucket) {
    return (data.put(bucket.bucketName, new HashMap<String, byte[]>()) == null);
  }

  public Boolean hasXReproxy() {
    return false;
  }

  public URL[] getRedirectURLs(Object object) {
    return new URL[]{}; // since the filesystem is not reproxyable
  }

  public Boolean deleteBucket(Bucket bucket) {

    // TODO: what if it contains stuff?

    return (data.remove(bucket.bucketName) != null);
  }

  public Boolean saveUploadedObject(Bucket bucket, UploadInfo uploadInfo, Object object) {

    byte[] tempContent = tempdata.get(uploadInfo.getTempLocation());
    data.get(bucket.bucketName).put(uploadInfo.getChecksum(), tempContent);
    return (tempdata.remove(uploadInfo.getTempLocation()) != null);

  }

  public Boolean deleteObject(Object object) {

    if (!objectExists(object))
      return false;

    return data.get(object.getBucketName()).remove(object.getChecksum()) != null;

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
