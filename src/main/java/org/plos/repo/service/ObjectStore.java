package org.plos.repo.service;

import org.plos.repo.models.Bucket;
import org.plos.repo.models.Object;

import java.io.InputStream;
import java.net.URL;
import java.util.regex.Pattern;

public abstract class ObjectStore {

  public static interface UploadInfo {
    Long getSize();
    String getTempLocation();
    String getChecksum();
  }

  public static final String digestAlgorithm = "SHA-1";


  public static boolean isValidFileName(String name) {
    return !Pattern.compile("[^-_.A-Za-z0-9]").matcher(name).find();
  }

//  public static boolean isValidBucketName(String bucketName) {
//    return BucketNameUtils.isValidV2BucketName(bucketName);
//  }

  public static String checksumToString(byte[] checksum) {

    StringBuilder sb = new StringBuilder();

    for (byte b : checksum)
      sb.append(Integer.toHexString((b & 0xFF) | 0x100).substring(1,3));

    return sb.toString();
  }

  abstract public Boolean hasXReproxy();

  abstract public URL[] getRedirectURLs(org.plos.repo.models.Object object) throws RepoException;

  abstract public Boolean objectExists(Object object);

  // we use Boolean here
  abstract public Boolean bucketExists(Bucket bucket);

  abstract public Boolean createBucket(Bucket bucket);

  abstract public Boolean deleteBucket(Bucket bucket);

  abstract public UploadInfo uploadTempObject(InputStream uploadedInputStream) throws RepoException;

  abstract public Boolean saveUploadedObject(Bucket bucket, UploadInfo uploadInfo, Object object);

  abstract public Boolean deleteObject(Object object);

  abstract public Boolean deleteTempUpload(UploadInfo uploadInfo);

  abstract public InputStream getInputStream(Object object) throws RepoException;

}
