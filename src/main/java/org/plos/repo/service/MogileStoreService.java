package org.plos.repo.service;

import com.guba.mogilefs.MogileFS;
import com.guba.mogilefs.PooledMogileFSImpl;
import org.plos.repo.models.Asset;
import org.plos.repo.models.Bucket;
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

  public boolean assetExists(Asset asset) {
    try {
      InputStream in = mfs.getFileStream(getAssetLocationString(asset.bucketName, asset.checksum));

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
    return true;  // TODO: make this configurable ?
  }

  public URL[] getRedirectURLs(Asset asset) throws Exception {

    String[] paths = mfs.getPaths(getAssetLocationString(asset.bucketName, asset.checksum), true);
    int pathCount = paths.length;
    URL[] urls = new URL[pathCount];

    for(int i = 0; i < pathCount; i++) {
      urls[i] = new URL(paths[i]);
    }

    return urls;
  }

  public InputStream getInputStream(Asset asset) throws Exception {
    return mfs.getFileStream(getAssetLocationString(asset.bucketName, asset.checksum));
  }

  public boolean createBucket(Bucket bucket) {
    // we use file paths instead of domains so this function does not need to do anything
    return true;
  }

  public boolean deleteBucket(Bucket bucket) {
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

  public boolean saveUploadedAsset(Bucket bucket, UploadInfo uploadInfo) throws Exception {
    try {
      mfs.rename(uploadInfo.getTempLocation(), getAssetLocationString(bucket.bucketName, uploadInfo.getChecksum()));
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public boolean deleteTempUpload(UploadInfo uploadInfo) {
    try {
      mfs.delete(uploadInfo.getTempLocation());
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public boolean deleteAsset(Asset asset) {
    try {
      mfs.delete(getAssetLocationString(asset.bucketName, asset.checksum));
      return true;
    } catch (Exception e) {
      return false;
    }
  }

}
