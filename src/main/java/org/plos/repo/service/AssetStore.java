package org.plos.repo.service;

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

  public static String checksumToString(byte[] checksum) {

    StringBuilder sb = new StringBuilder();

    for (byte b : checksum)
      sb.append(Integer.toHexString((b & 0xFF) | 0x100).substring(1,3));

    return sb.toString();
  }

  abstract public Boolean hasXReproxy();

  abstract public URL[] getRedirectURLs(String bucketName, String checksum) throws Exception;

  abstract public boolean assetExists(String bucketName, String checksum);

  abstract public boolean createBucket(String bucketName);

  abstract public boolean deleteBucket(String bucketName);

  abstract public UploadInfo uploadTempAsset(MultipartFile file) throws Exception;

  abstract public boolean saveUploadedAsset(String bucketName, String checksum, String tempFileLocation) throws Exception;

  abstract public boolean deleteAsset(String bucketName, String fileName);

  abstract public boolean deleteTempUpload(String tempLocation);

  abstract public InputStream getInputStream(String bucketName, String checksum) throws Exception;

}
