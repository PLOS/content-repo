package org.plos.repo.service;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
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

  public boolean assetExists(String bucketName, String checksum) {
    try {
      return s3Client.getObject(bucketName, checksum) != null;
    } catch (Exception e) {
      return false;
    }
  }

  public Boolean hasXReproxy() {
    return true;  // TODO: make this configurable
  }

  public URL[] getRedirectURLs(String bucketName, String checksum) throws Exception {
    return new URL[]{ new URL(s3Client.getResourceUrl(bucketName, checksum)) };
  }

  public InputStream getInputStream(String bucketName, String checksum) throws Exception {
    return s3Client.getObject(bucketName, checksum).getObjectContent();
  }

  public boolean createBucket(String bucketName) {

    try {
      CreateBucketRequest bucketRequest = new CreateBucketRequest(bucketName);
      bucketRequest.withCannedAcl(CannedAccessControlList.PublicRead);
      s3Client.createBucket(bucketRequest);

      return true;
    } catch (Exception e) {
      log.error("Error creating bucket", e);
      return false;
    }

  }

  public boolean deleteBucket(String bucketName) {

    try {
      s3Client.deleteBucket(bucketName);
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

  public boolean saveUploadedAsset(String bucketName, String checksum, String tempFileLocation) throws Exception {
    try {
      File tempFile = new File(tempFileLocation);

      // TODO: store some redundant metadata in case we loose the DB ?
      //ObjectMetadata metadata = new ObjectMetadata();
      //metadata.addUserMetadata("downloadName", );

      PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, checksum, tempFile);
      putObjectRequest.withCannedAcl(CannedAccessControlList.PublicRead);

      s3Client.putObject(putObjectRequest);

      tempFile.delete();

      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public boolean deleteTempUpload(String tempLocation) {
    return new File(tempLocation).delete();
  }

  public boolean deleteAsset(String bucketName, String fileName) {
    try {
      s3Client.deleteObject(bucketName, fileName);
      return true;
    } catch (Exception e) {
      log.error("Error deleting asset", e);
      return false;
    }
  }
}
