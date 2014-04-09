package org.plos.repo.service;

import com.guba.mogilefs.MogileFS;
import com.guba.mogilefs.PooledMogileFSImpl;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

public class MogileStoreService extends ObjectStore {

  private static final Logger log = LoggerFactory.getLogger(MogileStoreService.class);

  public static final String mogileFileClass = "";

//  public static final String mogileDefaultDomain = "toast";

  private MogileFS mfs = null;

  public MogileStoreService(String domain, String[] trackerStrings, int maxTrackerConnections, int maxIdleConnections, long maxIdleTimeMillis) throws Exception {
    mfs = new PooledMogileFSImpl(domain, trackerStrings, maxTrackerConnections, maxIdleConnections, maxIdleTimeMillis);
  }

  private String getBucketLocationString(String bucketName) {
    return bucketName + "/";
  }

  private String getObjectLocationString(String bucketName, String checksum) {
    return getBucketLocationString(bucketName) + checksum;
  }

  public boolean objectExists(Object object) {
    try {
      InputStream in = mfs.getFileStream(getObjectLocationString(object.bucketName, object.checksum));

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

  public URL[] getRedirectURLs(Object object) throws Exception {

    String[] paths = mfs.getPaths(getObjectLocationString(object.bucketName, object.checksum), true);
    int pathCount = paths.length;
    URL[] urls = new URL[pathCount];

    for(int i = 0; i < pathCount; i++) {
      urls[i] = new URL(paths[i]);
    }

    return urls;
  }

  public InputStream getInputStream(org.plos.repo.models.Object object) throws Exception {
    return mfs.getFileStream(getObjectLocationString(object.bucketName, object.checksum));
  }

  public boolean createBucket(Bucket bucket) {
    // we use file paths instead of domains so this function does not need to do anything
    return true;
  }

  public boolean deleteBucket(Bucket bucket) {
    return true;
  }

  public UploadInfo uploadTempObject(final MultipartFile file) throws Exception {
    final String tempFileLocation = UUID.randomUUID().toString() + ".tmp";

    OutputStream fos = mfs.newFile(tempFileLocation, mogileFileClass, file.getSize());

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

  public boolean saveUploadedObject(Bucket bucket, UploadInfo uploadInfo, Object object) throws Exception {
    try {
      mfs.rename(uploadInfo.getTempLocation(), getObjectLocationString(bucket.bucketName, uploadInfo.getChecksum()));
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

  public boolean deleteObject(Object object) {
    try {
      mfs.delete(getObjectLocationString(object.bucketName, object.checksum));
      return true;
    } catch (Exception e) {
      return false;
    }
  }

}
