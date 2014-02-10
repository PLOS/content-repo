package org.plos.repo.service;

import org.springframework.web.multipart.MultipartFile;

import java.util.Map;

public interface AssetStore {

  public String getAssetLocationString(String bucketName, String checksum);

  public boolean assetExists(String bucketName, String checksum);

  public boolean createBucket(String bucketName);

  public boolean deleteBucket(String bucketName);

  public Map.Entry<String, String> uploadTempAsset(MultipartFile file) throws Exception;

  public boolean saveUploadedAsset(String bucketName, String checksum, String tempFileLocation) throws Exception;

  public boolean deleteAsset(String fileLocation);

}
