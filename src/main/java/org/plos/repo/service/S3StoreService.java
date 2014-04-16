package org.plos.repo.service;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class S3StoreService extends ObjectStore {

  private static final Logger log = LoggerFactory.getLogger(S3StoreService.class);

  private AmazonS3Client s3Client = null;

  private static final String temp_upload_dir = "/tmp";

  public S3StoreService(String aws_access_key, String aws_secret_key) {
    s3Client = new AmazonS3Client(new BasicAWSCredentials(aws_access_key, aws_secret_key));
  }

  public boolean objectExists(Object object) {
    try {
      S3Object obj = s3Client.getObject(object.bucketName, object.checksum);

      if (obj == null)
        return false;

      obj.close();
      return true;

    } catch (Exception e) {
      return false;
    }
  }

  public Boolean hasXReproxy() {
    return true;  // TODO: make this configurable
  }

  public URL[] getRedirectURLs(Object object) throws Exception {
    return new URL[]{ new URL(s3Client.getResourceUrl(object.bucketName, object.checksum)) };
  }

  public InputStream getInputStream(Object object) throws Exception {
    return s3Client.getObject(object.bucketName, object.checksum).getObjectContent();
  }

  public boolean createBucket(Bucket bucket) {

    try {
      CreateBucketRequest bucketRequest = new CreateBucketRequest(bucket.bucketName, Region.US_West);
      bucketRequest.withCannedAcl(CannedAccessControlList.PublicRead);
      s3Client.createBucket(bucketRequest);

      return true;
    } catch (Exception e) {
      log.error("Error creating bucket", e);
      return false;
    }

  }

  public boolean deleteBucket(Bucket bucket) {

    try {
      s3Client.deleteBucket(bucket.bucketName);
      return true;
    } catch (Exception e) {
      log.error("Error deleting bucket", e);
      return false;
    }
  }

  /**
   * Upload the file to the local server, calculate the checksum. Dont put in on S3 yet.
   *
   * @param file
   * @return
   * @throws Exception
   */
  public UploadInfo uploadTempObject(InputStream uploadedInputStream) throws Exception {

    final String tempFileLocation = temp_upload_dir + "/" + UUID.randomUUID().toString() + ".tmp";

    FileOutputStream fos = new FileOutputStream(tempFileLocation);

    ReadableByteChannel in = Channels.newChannel(uploadedInputStream);
    MessageDigest digest = MessageDigest.getInstance(digestAlgorithm);
    WritableByteChannel out = Channels.newChannel(new DigestOutputStream(fos, digest));
    ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);

    long size = 0;

    while (in.read(buffer) != -1) {
      buffer.flip();
      size += out.write(buffer);
      buffer.clear();
    }

    final String checksum = checksumToString(digest.digest());
    final long finalSize = size;

    out.close();
    in.close();

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

  public boolean saveUploadedObject(Bucket bucket, UploadInfo uploadInfo, Object object) {

    int retries = 5;
    int tryCount = 0;
    int waitSecond = 4;

    ObjectMapper m = new ObjectMapper();
    Map<String, java.lang.Object> propsObj = m.convertValue(object, Map.class);

    Map<String, String> propsStr = new HashMap<>();

    for (Map.Entry<String, java.lang.Object> entry : propsObj.entrySet()) {
      try {
        if (entry.getValue() == null)
          propsStr.put(entry.getKey(), "");
        else
          propsStr.put(entry.getKey(), entry.getValue().toString());
      } catch (ClassCastException cce){
        log.error("Problem converting object to metadata", cce);
      }
    }

    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(uploadInfo.getSize());
    objectMetadata.setUserMetadata(propsStr);

    File tempFile = new File(uploadInfo.getTempLocation());

    PutObjectRequest putObjectRequest = new PutObjectRequest(bucket.bucketName, uploadInfo.getChecksum(), tempFile);
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
        } catch (Exception e2) {  }

      }
    }

    return false;
  }

  public boolean deleteTempUpload(UploadInfo uploadInfo) {
    return new File(uploadInfo.getTempLocation()).delete();
  }

  public boolean deleteObject(Object object) {
    try {
      s3Client.deleteObject(object.bucketName, object.checksum);
      return true;
    } catch (Exception e) {
      log.error("Error deleting object", e);
      return false;
    }
  }
}
