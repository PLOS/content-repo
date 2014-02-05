package org.plos.repo.service;

import org.springframework.web.multipart.MultipartFile;

import java.util.Date;
import java.util.Map;

public interface AssetStore {

  public String getAssetLocationString(String bucketName, String checksum, Date timestamp);

  public boolean createBucket(String bucketName);

  public boolean deleteBucket(String bucketName);

  public Map.Entry<String, String> uploadTempAsset(MultipartFile file) throws Exception;

  public boolean saveUploadedAsset(String bucketName, String checksum, String tempFileLocation, Date timestamp) throws Exception;

  public boolean deleteAsset(String fileLocation);

}
