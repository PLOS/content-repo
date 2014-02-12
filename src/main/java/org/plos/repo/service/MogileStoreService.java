package org.plos.repo.service;

import com.guba.mogilefs.MogileFS;
import com.guba.mogilefs.PooledMogileFSImpl;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.UUID;

public class MogileStoreService implements AssetStore {

  public static final String digestAlgorithm = "SHA-1";

  private MogileFS mfs;

  @Required
  public void setPreferences(Preferences preferences) throws Exception {

    mfs = null;
    mfs = new PooledMogileFSImpl("toast", new String[] { "127.0.0.1:7001" }, 0, 1, 10000);
//    data_dir = preferences.getDataDirectory();

  }

  public static String checksumToString(byte[] checksum) {

    StringBuilder sb = new StringBuilder();

    for (byte b : checksum)
      sb.append(Integer.toHexString((b & 0xFF) | 0x100).substring(1,3));

    return sb.toString();
  }

  private String getBucketLocationString(String bucketName) {
    return bucketName + "/";
  }

  public String getAssetLocationString(String bucketName, String checksum) {
    return getBucketLocationString(bucketName) + checksum;
  }

  public boolean assetExists(String bucketName, String checksum) {
    try {
      InputStream in = mfs.getFileStream(getAssetLocationString(bucketName, checksum));

      if (in == null) {
        in.close();
        return false;
      }

      in.close();
      return true;

    } catch (Exception e) {
      return false;
    }
  }

  public InputStream getInputStream(String bucketName, String checksum) throws Exception {
    return mfs.getFileStream(getAssetLocationString(bucketName, checksum));
  }

  public boolean createBucket(String bucketName) {
    return true;
  }

  public boolean deleteBucket(String bucketName) {
    return true;
  }

  public UploadInfo uploadTempAsset(final MultipartFile file) throws Exception {
    final String tempFileLocation = UUID.randomUUID().toString() + ".tmp";

    OutputStream fos = mfs.newFile(tempFileLocation, "testclass", file.getSize());

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

    out.close();
    in.close();
//    fos.close();


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
      mfs.rename(tempFileLocation, getAssetLocationString(bucketName, checksum));
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public boolean deleteAsset(String fileLocation) {
    try {
      mfs.delete(fileLocation);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
