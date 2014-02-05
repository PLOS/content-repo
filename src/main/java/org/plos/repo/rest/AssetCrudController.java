/*
 * Copyright (c) 2006-2014 by Public Library of Science
 * http://plos.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.plos.repo.rest;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.plos.repo.models.Asset;
import org.plos.repo.service.AssetStore;
import org.plos.repo.service.FileSystemStoreService;
import org.plos.repo.service.HsqlService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/assets")
public class AssetCrudController {

  private static final Logger log = LoggerFactory.getLogger(AssetCrudController.class);

  @Autowired
  private AssetStore assetStore;

  @Autowired
  private HsqlService hsqlService;

  private final static String defaultContentType = "application/octet-stream";


  // TODO: check at startup that db is in sync with assetStore


  @RequestMapping(method=RequestMethod.GET)
  public @ResponseBody List<Asset> list() throws Exception {
    return hsqlService.listAssets();
  }

  @RequestMapping(value = "/count", method = RequestMethod.GET)
  public @ResponseBody Integer count() throws Exception {
    return hsqlService.listAssets().size();
  }

  @RequestMapping(value="{bucketName}", method=RequestMethod.GET)
  public @ResponseBody
  void read(@PathVariable String bucketName,
            @RequestParam(required = true) String key,
            @RequestParam(required = false) String checksum,
            HttpServletResponse response) throws Exception {

    Asset asset;
    if (checksum == null)
      asset = hsqlService.getAsset(bucketName, key);
    else
      asset = hsqlService.getAsset(bucketName, key, checksum);

    if (asset != null) {

      if (checksum == null)
        checksum = asset.checksum;

      String exportFileName = "content";

      if (asset.downloadName != null)
        exportFileName = asset.downloadName;
      else if (FileSystemStoreService.isValidFileName(asset.key))
        exportFileName = asset.key;

      try {

        if (asset.contentType == null || asset.contentType.isEmpty())
          response.setContentType(defaultContentType);
        else
          response.setContentType(asset.contentType);
        response.setHeader("Content-Disposition", "inline; filename=" + exportFileName);

        InputStream is = new FileInputStream(assetStore.getAssetLocationString(bucketName, checksum,  asset.timestamp));
        IOUtils.copy(is, response.getOutputStream());
        response.flushBuffer();
      } catch (IOException ex) {
        log.info("Error writing file to output stream.", ex);
      }
    }
  }

  @RequestMapping(value="{bucketName}/{key}/{checksum}", method=RequestMethod.DELETE)
  public @ResponseBody String delete(@PathVariable String bucketName,
                                     @PathVariable String key,
                                     @PathVariable String checksum) throws Exception {

    Asset asset = hsqlService.getAsset(bucketName, key, checksum);
    Date timestamp = asset.timestamp;

    if (hsqlService.deleteAsset(key, checksum, bucketName) == 0)
      return "Error: can not find asset in database";

    if (!assetStore.deleteAsset(assetStore.getAssetLocationString(bucketName, checksum, timestamp)))
      return "Error: there was a problem deleting the asset from the filesystem";

    return checksum + " deleted";
  }

  /**
   * Adds an asset if the key does not already exist.
   *
   * @param key
   * @param bucketName
   * @param contentType
   * @param downloadName
   * @param file
   * @return
   * @throws Exception
   */
  @RequestMapping(method=RequestMethod.POST)
  public @ResponseBody String create(@RequestParam String key,
                                     @RequestParam String bucketName,
                                     @RequestParam String contentType,
                                     @RequestParam(required = false) String downloadName,
                                     @RequestParam MultipartFile file) throws Exception {
    return addAsset(key, bucketName, contentType, downloadName, file, true);
  }

  /**
   * Adds an asset version only if the key already exists.
   *
   * @param key
   * @param bucketName
   * @param contentType
   * @param downloadName
   * @param file
   * @return
   * @throws Exception
   */
  @RequestMapping(method=RequestMethod.PUT)
  public @ResponseBody String update(@RequestParam String key,
                                     @RequestParam String bucketName,
                                     @RequestParam String contentType,
                                     @RequestParam(required = false) String downloadName,
                                     @RequestParam MultipartFile file) throws Exception {
    return addAsset(key, bucketName, contentType, downloadName, file, false);
  }

  private String addAsset(String key,
                          String bucketName,
                          String contentType,
                          String downloadName,
                          MultipartFile file,
                          boolean newKey) throws Exception {

    Integer bucketId = hsqlService.getBucketId(bucketName);

    if (file.isEmpty())
      return "Error: file is empty";

    if (bucketId == null)
      return "Error: can not find bucket " + bucketName;

    Asset existingAsset = hsqlService.getAsset(bucketName, key);

    if (newKey && existingAsset != null) { // create
      return "Error: that key already exist. Perhaps you should use 'update' instead";
    } else if (existingAsset == null) {  // update
      return "Error: that key does not exist. Perhaps you should use 'create' instead";
    }

    Map.Entry<String, String> uploadResult = assetStore.uploadTempAsset(file);  // TODO: make sure this is successful
    String tempFileLocation = uploadResult.getKey();
    String checksum = uploadResult.getValue();
    File tempFile = new File(tempFileLocation);
    long fileSize = tempFile.length();

    Asset existingAssetVersion = hsqlService.getAsset(bucketName, key, checksum, fileSize);

    if (existingAssetVersion != null) {

      File existingFile = new File(assetStore.getAssetLocationString(bucketName, checksum, existingAssetVersion.timestamp));

      if (FileUtils.contentEquals(tempFile, existingFile)) {
        assetStore.deleteAsset(tempFileLocation);
        log.info("skipping insert since asset version already exists");
      }
    } else {
      Date timestamp = new Date();
      assetStore.saveUploadedAsset(bucketName, checksum, tempFileLocation, timestamp);
      log.info("insert: " + hsqlService.insertAsset(key, checksum, bucketId, contentType, downloadName, fileSize, timestamp));
    }

    return checksum;
  }

}
