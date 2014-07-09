package org.plos.repo.service;

import com.google.common.base.Preconditions;
import com.guba.mogilefs.MogileFS;
import com.guba.mogilefs.PooledMogileFSImpl;
import org.apache.commons.io.IOUtils;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.security.MessageDigest;
import java.util.UUID;

public class MogileStoreService extends ObjectStore {

  private static final Logger log = LoggerFactory.getLogger(MogileStoreService.class);

  public static final String mogileFileClass = "";

  private MogileFS mfs = null;

  public MogileStoreService(String domain, String[] trackerStrings, int maxTrackerConnections, int maxIdleConnections, long maxIdleTimeMillis) throws Exception {
    mfs = new PooledMogileFSImpl(domain, trackerStrings, maxTrackerConnections, maxIdleConnections, maxIdleTimeMillis);
  }

  private static byte[] readStreamInput(InputStream input) throws IOException {
    Preconditions.checkNotNull(input);
    try {
      return IOUtils.toByteArray(input);
    } finally {
      input.close();
    }
  }

  private String getObjectLocationString(String bucketName, String checksum) {
    return checksum + "-" + bucketName;
  }

  public boolean objectExists(Object object) {
    try {
      InputStream in = mfs.getFileStream(getObjectLocationString(object.bucketName, object.checksum));

      if (in == null)
        return false;

      in.close();
      return true;

    } catch (Exception e) {
      return false;
    }
  }

  public Boolean hasXReproxy() {
    return true;
  }

  public URL[] getRedirectURLs(Object object) throws RepoException {

    try {
      String[] paths = mfs.getPaths(getObjectLocationString(object.bucketName, object.checksum), true);
      int pathCount = paths.length;
      URL[] urls = new URL[pathCount];

      for (int i = 0; i < pathCount; i++) {
        urls[i] = new URL(paths[i]);
      }
      return urls;

    } catch (Exception e) {
      throw new RepoException(e);
    }

  }

  public InputStream getInputStream(org.plos.repo.models.Object object) throws RepoException {
    try {
      return mfs.getFileStream(getObjectLocationString(object.bucketName, object.checksum));
    } catch (Exception e) {
      throw new RepoException(e);
    }
  }

  public boolean bucketExists(Bucket bucket) {
    return true;
  }

  public boolean createBucket(Bucket bucket) {
    // we use file paths instead of domains so this function does not need to do anything
    return true;
  }

  public boolean deleteBucket(Bucket bucket) {
    return true;
  }

  public UploadInfo uploadTempObject(InputStream uploadedInputStream) throws RepoException {

    final String tempFileLocation = UUID.randomUUID().toString() + ".tmp";

    // NOTE: in the future we can avoid having to read to memory by using a
    //   different MogileFS library that does not require size at creation time.

    try {
      byte[] objectData = readStreamInput(uploadedInputStream);

      MessageDigest digest = MessageDigest.getInstance(digestAlgorithm);

      OutputStream fos = mfs.newFile(tempFileLocation, mogileFileClass, objectData.length);

      IOUtils.write(objectData, fos);
      fos.close();

      final String checksum = checksumToString(digest.digest(objectData));
      final long finalSize = objectData.length;

      return new UploadInfo() {
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

    } catch (Exception e) {
      throw new RepoException(e);
    }
  }

  public boolean saveUploadedObject(Bucket bucket, UploadInfo uploadInfo, Object object) {
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
