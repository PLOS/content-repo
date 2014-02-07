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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.plos.repo.models.Asset;
import org.plos.repo.service.AssetStore;
import org.plos.repo.service.FileSystemStoreService;
import org.plos.repo.service.HsqlService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
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
import java.sql.Timestamp;
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


  // TODO: check at startup that db is in sync with assetStore ?


  @RequestMapping(method=RequestMethod.GET)
  public @ResponseBody List<Asset> listAllAssets() throws Exception {
    return hsqlService.listAllAssets();
  }

//  @RequestMapping(value="{bucketName}", method=RequestMethod.GET)
//  public @ResponseBody List<Asset> listAssetsInBucket(@PathVariable String bucketName) throws Exception {
//    return hsqlService.listAssetsInBucket(bucketName);
//  }

  @RequestMapping(value = "/count", method = RequestMethod.GET)
  public @ResponseBody Integer count() throws Exception {
    return hsqlService.listAllAssets().size();
  }

  @RequestMapping(value="{bucketName}", method=RequestMethod.GET)
  public @ResponseBody
  void read(@PathVariable String bucketName,
            @RequestParam(required = true) String key,
            @RequestParam(required = false) String checksum,
            @RequestParam(required = false) Boolean fetchMetadata,
            HttpServletResponse response) throws IOException {

    Asset asset;
    if (checksum == null)
      asset = hsqlService.getAsset(bucketName, key);
    else
      asset = hsqlService.getAsset(bucketName, key, checksum);

    if (asset == null) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      return;
    }

    if (fetchMetadata != null && fetchMetadata) {

      response.setContentType(MediaType.APPLICATION_JSON_VALUE);
      //Gson gson = new new Gson();
      Gson gson = new GsonBuilder().setDateFormat("yyyy").create();

      JsonObject jsonObject = gson.toJsonTree(asset).getAsJsonObject();

      // get the list of versions
      if (checksum == null)
        jsonObject.add("versions", gson.toJsonTree(hsqlService.listAssetVersions(bucketName, key)).getAsJsonArray());


      response.getWriter().write(jsonObject.toString());
      response.setStatus(HttpServletResponse.SC_OK);

      return;
    }

    // if they want the binary data

    if (checksum == null)
      checksum = asset.checksum;

    String exportFileName = "content";

    if (asset.downloadName != null)
      exportFileName = asset.downloadName;
    else if (FileSystemStoreService.isValidFileName(asset.key))
      exportFileName = asset.key;

    try {

      if (asset.contentType == null || asset.contentType.isEmpty())
        response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
      else
        response.setContentType(asset.contentType);
      response.setHeader("Content-Disposition", "inline; filename=" + exportFileName);

      InputStream is = new FileInputStream(assetStore.getAssetLocationString(bucketName, checksum));
      IOUtils.copy(is, response.getOutputStream());
      response.setStatus(HttpServletResponse.SC_FOUND);
      response.flushBuffer();
    } catch (IOException ex) {
      response.setStatus(HttpServletResponse.SC_EXPECTATION_FAILED);
      log.info("Error writing file to output stream.", ex);
    }

  }

  @RequestMapping(value="{bucketName}", method=RequestMethod.DELETE)
  public ResponseEntity<String> delete(@PathVariable String bucketName,
                                       @RequestParam String key,
                                       @RequestParam String checksum,
                                       @RequestParam long timestamp) throws Exception {

    if (hsqlService.deleteAsset(key, checksum, bucketName, new Timestamp(timestamp)) == 0)
      return new ResponseEntity<>("Error: Can not find asset in database.", HttpStatus.NOT_FOUND);

    // delete it from the asset store if it is no longer referenced in the database
    if (!hsqlService.assetInUse(bucketName, checksum) && !assetStore.deleteAsset(assetStore.getAssetLocationString(bucketName, checksum)))
      return new ResponseEntity<>("Error: There was a problem deleting the asset from the filesystem.", HttpStatus.NOT_MODIFIED);

    return new ResponseEntity<>(checksum + " deleted", HttpStatus.OK);
  }

  @RequestMapping(method=RequestMethod.POST)
  public ResponseEntity<String> createOrUpdate(@RequestParam String key,
                                               @RequestParam String bucketName,
                                               @RequestParam(required = false) String contentType,
                                               @RequestParam(required = false) String downloadName,
                                               @RequestParam(required = false) MultipartFile file,
                                               @RequestParam(required = true) boolean newAsset
  ) throws Exception {

    if (newAsset)
      return create(key, bucketName, contentType, downloadName, file);

    return update(key, bucketName, contentType, downloadName, file);
  }

  private ResponseEntity<String> create(String key,
                          String bucketName,
                          String contentType,
                          String downloadName,
                          MultipartFile file) throws Exception {

    Integer bucketId = hsqlService.getBucketId(bucketName);

    if (file == null)
      return new ResponseEntity<>("Error: A file must be specified for uploading.", HttpStatus.PRECONDITION_FAILED);

    if (bucketId == null)
      return new ResponseEntity<>("Error: Can not find bucket " + bucketName, HttpStatus.INSUFFICIENT_STORAGE);

    Asset existingAsset = hsqlService.getAsset(bucketName, key);

    if (existingAsset != null)
      return new ResponseEntity<>("Error: Attempting to create an asset with a key that already exists.", HttpStatus.CONFLICT);

    Map.Entry<String, String> uploadResult = assetStore.uploadTempAsset(file);  // TODO: make sure this is successful
    String tempFileLocation = uploadResult.getKey();
    String checksum = uploadResult.getValue();
    File tempFile = new File(tempFileLocation);
    long fileSize = tempFile.length();

    HttpStatus status = HttpStatus.CREATED; // status indicates if it made it to the DB, not the asset store

    // determine if the asset should be added to the store or not
    if (hsqlService.assetInUse(bucketName, checksum)) {

      if (FileUtils.contentEquals(tempFile, new File(assetStore.getAssetLocationString(bucketName, checksum)))) {
        log.info("not adding asset to store since content exists");
      } else {
        log.info("checksum collision!!");
        status = HttpStatus.CONFLICT;
      }

      // dont bother storing the file since the data already exists in the system
      assetStore.deleteAsset(tempFileLocation);
    } else {
      assetStore.saveUploadedAsset(bucketName, checksum, tempFileLocation);
    }

    // add a record to the DB
    log.info("db asset inserts: " + hsqlService.insertAsset(key, checksum, bucketId, contentType, downloadName, fileSize, new Timestamp(new Date().getTime())));

    return new ResponseEntity<>(checksum, status);
  }

  private ResponseEntity<String> update(String key,
                                        String bucketName,
                                        String contentType,
                                        String downloadName,
                                        MultipartFile file) throws Exception {

    Integer bucketId = hsqlService.getBucketId(bucketName);

    if (bucketId == null)
      return new ResponseEntity<>("Error: Can not find bucket " + bucketName, HttpStatus.INSUFFICIENT_STORAGE);

    Asset existingAsset = hsqlService.getAsset(bucketName, key);

    if (existingAsset == null)
      return new ResponseEntity<>("Error: Attempting to create a new version of an non-existing asset.", HttpStatus.NOT_ACCEPTABLE);

    // copy over values from previous asset, if they are not specified in the request
    if (contentType == null)
      contentType = existingAsset.contentType;

    if (downloadName == null)
      downloadName = existingAsset.downloadName;

    if (file == null) {
      // TODO: duplicate file?
      log.info("db asset inserts: " + hsqlService.insertAsset(key, existingAsset.checksum, bucketId, contentType, downloadName, existingAsset.size, existingAsset.timestamp));

      return new ResponseEntity<>(existingAsset.checksum, HttpStatus.OK);
    }

    Map.Entry<String, String> uploadResult = assetStore.uploadTempAsset(file);  // TODO: make sure this is successful
    String tempFileLocation = uploadResult.getKey();
    String checksum = uploadResult.getValue();
    File tempFile = new File(tempFileLocation);
    long fileSize = tempFile.length();

    HttpStatus status = HttpStatus.OK; // note, different from 'create'

    // determine if the asset should be added to the store or not
    if (hsqlService.assetInUse(bucketName, checksum)) {

      if (FileUtils.contentEquals(tempFile, new File(assetStore.getAssetLocationString(bucketName, checksum)))) {
        log.info("not adding asset to store since content exists");
      } else {
        log.info("checksum collision!!");
        status = HttpStatus.CONFLICT;
      }

      // dont bother storing the file since the data already exists in the system
      assetStore.deleteAsset(tempFileLocation);
    } else {
      assetStore.saveUploadedAsset(bucketName, checksum, tempFileLocation);
    }

    // add a record to the DB
    log.info("db asset inserts: " + hsqlService.insertAsset(key, checksum, bucketId, contentType, downloadName, fileSize, new Timestamp(new Date().getTime())));

    return new ResponseEntity<>(checksum, status);
  }

}
