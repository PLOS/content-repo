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

public class MogileStoreService extends AssetStore {

  public static final String mogileFileClass = "";

  private MogileFS mfs = null;

  @Required
  public void setPreferences(Preferences preferences) throws Exception {

    // TODO: figure out how to handle domains

    mfs = new PooledMogileFSImpl("toast", preferences.getMogileTrackers(), 1, 1, 100);
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

    // TODO: return location instead of stream?
    // ie - http://127.0.0.20:7500/dev1/0/000/000/0000000006.fid

    return mfs.getFileStream(getAssetLocationString(bucketName, checksum));
  }

  public boolean createBucket(String bucketName) {

    // TODO: use mogile domains instead of filepaths?

    return true;
  }

  public boolean deleteBucket(String bucketName) {
    return true;
  }

  public UploadInfo uploadTempAsset(final MultipartFile file) throws Exception {
    final String tempFileLocation = UUID.randomUUID().toString() + ".tmp";

    OutputStream fos = mfs.newFile(tempFileLocation, mogileFileClass, file.getSize());

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
