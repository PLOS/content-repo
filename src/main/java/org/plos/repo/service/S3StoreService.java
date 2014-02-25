package org.plos.repo.service;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.plos.repo.models.Asset;
import org.plos.repo.models.Bucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.web.multipart.MultipartFile;

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
import java.util.UUID;

public class S3StoreService extends AssetStore {

  private static final Logger log = LoggerFactory.getLogger(S3StoreService.class);

  private AmazonS3Client s3Client = null;

  private static final String temp_upload_dir = "/tmp";

  @Required
  public void setPreferences(Preferences preferences) throws Exception {
    s3Client = new AmazonS3Client(preferences.getAWScredentials());
  }

  public boolean assetExists(Asset asset) {
    try {
      return s3Client.getObject(asset.bucketName, asset.checksum) != null;
    } catch (Exception e) {
      return false;
    }
  }

  public Boolean hasXReproxy() {
    return true;  // TODO: make this configurable
  }

  public URL[] getRedirectURLs(Asset asset) throws Exception {
    return new URL[]{ new URL(s3Client.getResourceUrl(asset.bucketName, asset.checksum)) };
  }

  public InputStream getInputStream(Asset asset) throws Exception {
    return s3Client.getObject(asset.bucketName, asset.checksum).getObjectContent();
  }

  public boolean createBucket(Bucket bucket) {

    try {
      CreateBucketRequest bucketRequest = new CreateBucketRequest(bucket.bucketName, com.amazonaws.services.s3.model.Region.US_West);
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
  public UploadInfo uploadTempAsset(final MultipartFile file) throws Exception {

    final String tempFileLocation = temp_upload_dir + "/" + UUID.randomUUID().toString() + ".tmp";

    FileOutputStream fos = new FileOutputStream(tempFileLocation);

    ReadableByteChannel in = Channels.newChannel(file.getInputStream());
    MessageDigest digest = MessageDigest.getInstance(digestAlgorithm);
    WritableByteChannel out = Channels.newChannel(new DigestOutputStream(fos, digest));
    ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);

    while (in.read(buffer) != -1) {
      buffer.flip();
      out.write(buffer);
      buffer.clear();
    }

    final String checksum = checksumToString(digest.digest());

    out.close();
    in.close();

    return new UploadInfo(){
      @Override
      public Long getSize() {
        return file.getSize();
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

  public boolean saveUploadedAsset(Bucket bucket, UploadInfo uploadInfo) {

    int retries = 5;
    int tryCount = 0;
    int waitSecond = 4;

    File tempFile = new File(uploadInfo.getTempLocation());

    PutObjectRequest putObjectRequest = new PutObjectRequest(bucket.bucketName, uploadInfo.getChecksum(), tempFile);
    putObjectRequest.withCannedAcl(CannedAccessControlList.PublicRead);

    while (tryCount < retries) {

      try {

        s3Client.putObject(putObjectRequest);
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

  public boolean deleteAsset(Asset asset) {
    try {
      s3Client.deleteObject(asset.bucketName, asset.checksum);
      return true;
    } catch (Exception e) {
      log.error("Error deleting asset", e);
      return false;
    }
  }
}
