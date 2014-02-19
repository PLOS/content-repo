package org.plos.repo.service;

import org.plos.repo.models.Asset;
import org.plos.repo.models.Bucket;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.net.URL;
import java.util.regex.Pattern;

public abstract class AssetStore {

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

  abstract public URL[] getRedirectURLs(Asset asset) throws Exception;

  abstract public boolean assetExists(Asset asset);

  abstract public boolean createBucket(Bucket bucket);

  abstract public boolean deleteBucket(Bucket bucket);

  abstract public UploadInfo uploadTempAsset(MultipartFile file) throws Exception;

  abstract public boolean saveUploadedAsset(Bucket bucket, UploadInfo uploadInfo) throws Exception;

  abstract public boolean deleteAsset(Asset asset);

  abstract public boolean deleteTempUpload(UploadInfo uploadInfo);

  abstract public InputStream getInputStream(Asset asset) throws Exception;

}
