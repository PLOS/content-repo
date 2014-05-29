package org.plos.repo.service;

import org.apache.commons.io.IOUtils;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.UUID;

public class InMemoryFileStoreService extends ObjectStore {

  private static final Logger log = LoggerFactory.getLogger(InMemoryFileStoreService.class);

  // bucketName -> data checksum -> file content
  private HashMap<String, HashMap<String, byte[]>> data = new HashMap<>();

  private HashMap<String, byte[]> tempdata = new HashMap<>();

  public InMemoryFileStoreService() {
  }

  public boolean objectExists(Object object) {
    return (data.get(object.bucketName) != null && data.get(object.bucketName).get(object.checksum) != null);
  }

  public InputStream getInputStream(Object object) throws Exception {
    return new ByteArrayInputStream(data.get(object.bucketName).get(object.checksum));
  }

  public boolean createBucket(Bucket bucket) {
    return (data.put(bucket.bucketName, new HashMap<String, byte[]>()) == null);
  }

  public Boolean hasXReproxy() {
    return false;
  }

  public URL[] getRedirectURLs(Object object) {
    return new URL[]{}; // since the filesystem is not reproxyable
  }

  public boolean deleteBucket(Bucket bucket) {

    // TODO: what if it contains stuff?

    return (data.remove(bucket.bucketName) != null);
  }

  public boolean saveUploadedObject(Bucket bucket, UploadInfo uploadInfo, Object object)
      throws Exception {

    byte[] tempContent = tempdata.get(uploadInfo.getTempLocation());
    data.get(bucket.bucketName).put(uploadInfo.getChecksum(), tempContent);
    return (tempdata.remove(uploadInfo.getTempLocation()) != null);

  }

  public boolean deleteObject(Object object) {

    if (!objectExists(object))
      return false;

    return data.get(object.bucketName).remove(object.checksum) != null;

  }

  public boolean deleteTempUpload(UploadInfo uploadInfo) {
    tempdata.remove(uploadInfo.getTempLocation());

    return true;
  }


  public UploadInfo uploadTempObject(InputStream uploadedInputStream) throws Exception {

    MessageDigest digest = MessageDigest.getInstance(digestAlgorithm);

    final String tempFileLocation = UUID.randomUUID().toString() + ".tmp";

    byte[] bytes = IOUtils.toByteArray(uploadedInputStream);

    tempdata.put(tempFileLocation, bytes);

    final String checksum = checksumToString(digest.digest(bytes));
    final long finalSize = bytes.length;

    return new UploadInfo(){
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

  }

}
