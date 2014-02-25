package org.plos.repo.service;

import org.plos.repo.models.Asset;
import org.plos.repo.models.Bucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileInputStream;
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

public class FileSystemStoreService extends AssetStore {

  private static final Logger log = LoggerFactory.getLogger(FileSystemStoreService.class);

  private String data_dir;

  @Required
  public void setPreferences(Preferences preferences) {
    data_dir = preferences.getDataDirectory();
  }

  private String getBucketLocationString(String bucketName) {
    return data_dir + "/" + bucketName + "/";
  }

  public String getAssetLocationString(String bucketName, String checksum) {
    return getBucketLocationString(bucketName) + checksum.substring(0, 2) + "/" + checksum;
  }

  public boolean assetExists(Asset asset) {
    return new File(getAssetLocationString(asset.bucketName, asset.checksum)).exists();
  }

  public InputStream getInputStream(Asset asset) throws Exception {
    return new FileInputStream(getAssetLocationString(asset.bucketName, asset.checksum));
  }

  public boolean createBucket(Bucket bucket) {

    File dir = new File(getBucketLocationString(bucket.bucketName));
    boolean result = dir.mkdir();

    if (!result)
      log.debug("Error while creating bucket. Directory was not able to be created : " + getBucketLocationString(bucket.bucketName));

    return result;
  }

  public Boolean hasXReproxy() {
    return false;
  }

  public URL[] getRedirectURLs(Asset asset) {
    return new URL[]{}; // since the filesystem is not reproxyable
  }

  public boolean deleteBucket(Bucket bucket) {
    File dir = new File(getBucketLocationString(bucket.bucketName));
    return dir.delete();
  }

  public boolean saveUploadedAsset(Bucket bucket, UploadInfo uploadInfo)
  throws Exception {
    File tempFile = new File(uploadInfo.getTempLocation());

    File newFile = new File(getAssetLocationString(bucket.bucketName, uploadInfo.getChecksum()));

    // create the subdirectory if it does not exist
    File subDir = new File(newFile.getParent());
    if (!subDir.exists() && !subDir.mkdir())
      return false;

    return tempFile.renameTo(newFile);
  }

  public boolean deleteAsset(Asset asset) {

    File file = new File(getAssetLocationString(asset.bucketName, asset.checksum));
    File subDir = new File(file.getParent());

    boolean result = file.delete();

    // delete the parent subdirectory if it is empty

    if (subDir.isDirectory() && file.list().length == 0)
      subDir.delete();

    return result;
  }

  public boolean deleteTempUpload(UploadInfo uploadInfo) {
    return new File(uploadInfo.getTempLocation()).delete();
  }

  public UploadInfo uploadTempAsset(final MultipartFile file) throws Exception {
    final String tempFileLocation = data_dir + "/" + UUID.randomUUID().toString() + ".tmp";

    FileOutputStream fos = new FileOutputStream(tempFileLocation);

    ReadableByteChannel in = Channels.newChannel(file.getInputStream());
    MessageDigest digest = MessageDigest.getInstance(digestAlgorithm);
    WritableByteChannel out = Channels.newChannel(
        new DigestOutputStream(fos, digest));
    ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);

    while (in.read(buffer) != -1) {
      buffer.flip();
      out.write(buffer);
      buffer.clear();
    }

    final String checksum = checksumToString(digest.digest());

    in.close();
    out.close();

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

}
