package org.plos.repo.service;

import com.guba.mogilefs.MogileFS;
import com.guba.mogilefs.PooledMogileFSImpl;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.UUID;

public class MogileStoreService extends AssetStore {

  public static final String mogileFileClass = "";

  public static final String mogileDefaultDomain = "toast";

  private MogileFS mfs = null;

  @Required
  public void setPreferences(Preferences preferences) throws Exception {
    mfs = new PooledMogileFSImpl(mogileDefaultDomain, preferences.getMogileTrackers(), 1, 1, 100);
  }

  private String getBucketLocationString(String bucketName) {
    return bucketName + "/";
  }

  private String getAssetLocationString(String bucketName, String checksum) {
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

  public Boolean hasXReproxy() {
    return true;  // TODO: make this configurable
  }

  public URL[] getRedirectURLs(String bucketName, String checksum) throws Exception {

    String[] paths = mfs.getPaths(getAssetLocationString(bucketName, checksum), true);
    int pathCount = paths.length;
    URL[] urls = new URL[pathCount];

    for(int i = 0; i < pathCount; i++) {
      urls[i] = new URL(paths[i]);
    }

    return urls;
  }

  public InputStream getInputStream(String bucketName, String checksum) throws Exception {
    return mfs.getFileStream(getAssetLocationString(bucketName, checksum));
  }

  public boolean createBucket(String bucketName) {
    // we use file paths instead of domains so this function does not need to do anything
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

  public boolean deleteTempUpload(String tempLocation) {
    try {
      mfs.delete(tempLocation);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public boolean deleteAsset(String bucketName, String fileName) {
    try {
      mfs.delete(getAssetLocationString(bucketName, fileName));
      return true;
    } catch (Exception e) {
      return false;
    }
  }

}
