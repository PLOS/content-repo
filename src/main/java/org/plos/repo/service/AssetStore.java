package org.plos.repo.service;

import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;

public interface AssetStore {

  public static interface UploadInfo {
    Long getSize();
    String getTempLocation();
    String getChecksum();
  }

  public String getAssetLocationString(String bucketName, String checksum);

  public boolean assetExists(String bucketName, String checksum);

  public boolean createBucket(String bucketName);

  public boolean deleteBucket(String bucketName);

  public UploadInfo uploadTempAsset(MultipartFile file) throws Exception;

  public boolean saveUploadedAsset(String bucketName, String checksum, String tempFileLocation) throws Exception;

  public boolean deleteAsset(String fileLocation);

  public InputStream getInputStream(String bucketName, String checksum) throws Exception;

}
