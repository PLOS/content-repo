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

import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.commons.io.IOUtils;
import org.plos.repo.models.Asset;
import org.plos.repo.models.Bucket;
import org.plos.repo.service.AssetStore;
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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;

@Controller
@RequestMapping("/assets")
public class AssetCrudController {

  private static final Logger log = LoggerFactory.getLogger(AssetCrudController.class);

  private static final Joiner REPROXY_URL_JOINER = Joiner.on(' ');

  private static final int REPROXY_CACHE_FOR_VALUE = 6 * 60 * 60; // TODO: Make configurable

  private static final String REPROXY_CACHE_FOR_HEADER =
      REPROXY_CACHE_FOR_VALUE + "; Last-Modified Content-Type Content-Disposition";

  @Autowired
  private AssetStore assetStore;

  @Autowired
  private HsqlService hsqlService;


  // TODO: check at startup that db is in sync with assetStore ?

  private static Gson gson = new Gson();

  private String assetToJsonString(Asset asset, boolean includeVersions) {
    JsonObject jsonObject = gson.toJsonTree(asset).getAsJsonObject();

    if (includeVersions)
      jsonObject.add("versions", gson.toJsonTree(hsqlService.listAssetVersions(asset)).getAsJsonArray());

    return jsonObject.toString();
  }

  @RequestMapping(method=RequestMethod.GET)
  public @ResponseBody List<Asset> listAllAssets() throws Exception {
    return hsqlService.listAllAssets();
  }

//  @RequestMapping(value="{bucketName}", method=RequestMethod.GET)
//  public @ResponseBody List<Asset> listAssetsInBucket(@PathVariable String bucketName) throws Exception {
//    return hsqlService.listAssetsInBucket(bucketName);
//  }

  private boolean clientSupportsReproxy(HttpServletRequest request) {
    Enumeration<?> headers = request.getHeaders("X-Proxy-Capabilities");
    if (headers == null) {
      return false;
    }
    while (headers.hasMoreElements()) {
      if ("reproxy-file".equals(headers.nextElement())) {
        return true;
      }
    }
    return false;
  }

  @RequestMapping(value="{bucketName}", method=RequestMethod.GET)
  public @ResponseBody
  void read(@PathVariable String bucketName,
            @RequestParam(required = true) String key,
            @RequestParam(required = false) Integer version,
            @RequestParam(required = false) Boolean fetchMetadata,
            HttpServletRequest request,
            HttpServletResponse response) throws Exception {

    Asset asset;
    if (version == null)
      asset = hsqlService.getAsset(bucketName, key);
    else
      asset = hsqlService.getAsset(bucketName, key, version);

    if (asset == null) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      return;
    }

    // if they want the metadata

    if (fetchMetadata != null && fetchMetadata) {
      response.setContentType(MediaType.APPLICATION_JSON_VALUE);
      try {
        response.getWriter().write(assetToJsonString(asset, version == null));
        response.setStatus(HttpServletResponse.SC_OK);
      } catch (IOException e) {
        log.error("Error reading metadata", e);
        response.setStatus(HttpServletResponse.SC_EXPECTATION_FAILED);
      }
      return;
    }

    // if they want redirect URLs

    if (clientSupportsReproxy(request) && assetStore.hasXReproxy()) {

      response.setHeader("X-Reproxy-URL", REPROXY_URL_JOINER.join(assetStore.getRedirectURLs(asset)));
      response.setHeader("X-Reproxy-Cache-For", REPROXY_CACHE_FOR_HEADER);
      response.setStatus(HttpServletResponse.SC_OK);

      return;
    }

    // else assume they want the binary data

    String exportFileName = "content";

    if (asset.downloadName != null)
      exportFileName = asset.downloadName;
    else if (AssetStore.isValidFileName(asset.key))
      exportFileName = asset.key;

    try {

      if (asset.contentType == null || asset.contentType.isEmpty())
        response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
      else
        response.setContentType(asset.contentType);
      response.setHeader("Content-Disposition", "inline; filename=" + exportFileName);

      InputStream is = assetStore.getInputStream(asset);
      IOUtils.copy(is, response.getOutputStream());
      response.setStatus(HttpServletResponse.SC_FOUND);
      response.flushBuffer();
      is.close();
    } catch (Exception ex) {
      response.setStatus(HttpServletResponse.SC_EXPECTATION_FAILED);
      log.info("Error writing file to output stream.", ex);
    }

  }

  @RequestMapping(value="{bucketName}", method=RequestMethod.DELETE)
  public ResponseEntity<String> delete(@PathVariable String bucketName,
                                       @RequestParam String key,
                                       @RequestParam int version) throws Exception {

    if (hsqlService.markAssetDeleted(key, bucketName, version) == 0)
      return new ResponseEntity<>("Error: Can not find asset in database.", HttpStatus.NOT_FOUND);

    // NOTE: we no longer delete assets from the asset store

    // delete it from the asset store if it is no longer referenced in the database
//    if (!hsqlService.assetInUse(bucketName, checksum) && !assetStore.deleteAsset(assetStore.getAssetLocationString(bucketName, checksum)))
//      return new ResponseEntity<>("Error: There was a problem deleting the asset from the filesystem.", HttpStatus.NOT_MODIFIED);

    return new ResponseEntity<>(key + " version " + version + " deleted", HttpStatus.OK);
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

    AssetStore.UploadInfo uploadInfo;
    try {
      uploadInfo = assetStore.uploadTempAsset(file);
    } catch (Exception e) {
      log.error("Error during upload", e);
      return new ResponseEntity<>("Error: A problem occurred while uploading the file.", HttpStatus.PRECONDITION_FAILED);
    }

    HttpStatus status = HttpStatus.CREATED; // note: status indicates if it made it to the DB, not the asset store

    Asset asset = new Asset(null, key, uploadInfo.getChecksum(), new Timestamp(new Date().getTime()), downloadName, contentType, uploadInfo.getSize(), null, null, bucketId, bucketName, 0, Asset.Status.USED);

    // determine if the asset should be added to the store or not
    if (assetStore.assetExists(asset)) {

//      if (FileUtils.contentEquals(tempFile, new File(assetStore.getAssetLocationString(bucketName, checksum)))) {
//        log.info("not adding asset to store since content exists");
//      } else {
//        log.info("checksum collision!!");
//        status = HttpStatus.CONFLICT;
//      }

      // dont bother storing the file since the data already exists in the system
      assetStore.deleteTempUpload(uploadInfo);
    } else {
      assetStore.saveUploadedAsset(new Bucket(bucketName), uploadInfo);
      asset.urls = REPROXY_URL_JOINER.join(assetStore.getRedirectURLs(asset));
    }

    // add a record to the DB

    hsqlService.insertAsset(asset); // TODO: deal with 0 return values

    return new ResponseEntity<>(assetToJsonString(asset, false), status);
  }

  private ResponseEntity<String> update(String key,
                                        String bucketName,
                                        String contentType,
                                        String downloadName,
                                        MultipartFile file) throws Exception {

    Integer bucketId = hsqlService.getBucketId(bucketName);

    if (bucketId == null)
      return new ResponseEntity<>("Error: Can not find bucket " + bucketName, HttpStatus.INSUFFICIENT_STORAGE);

    Asset asset = hsqlService.getAsset(bucketName, key);

    if (asset == null)
      return new ResponseEntity<>("Error: Attempting to create a new version of an non-existing asset.", HttpStatus.NOT_ACCEPTABLE);

    // copy over values from previous asset, if they are not specified in the request
    if (contentType != null)
      asset.contentType = contentType;

    if (downloadName != null)
      asset.downloadName = downloadName;

    // TODO: wrap this in a transaction since versionNumber is being updated ?

    asset.timestamp = new Timestamp(new Date().getTime());
    asset.versionNumber++;
    asset.id = null;  // remove this since it refers to the old asset

    if (file == null) {
      asset.urls = REPROXY_URL_JOINER.join(assetStore.getRedirectURLs(asset));
      hsqlService.insertAsset(asset); // TODO: deal with 0 return values

      return new ResponseEntity<>(assetToJsonString(asset, false), HttpStatus.OK);
    }

    AssetStore.UploadInfo uploadInfo;
    try {
      uploadInfo = assetStore.uploadTempAsset(file);
    } catch (Exception e) {
      log.error("Error during upload", e);
      return new ResponseEntity<>("Error: A problem occurred while uploading the file.", HttpStatus.PRECONDITION_FAILED);
    }

    HttpStatus status = HttpStatus.OK; // note: different return value from 'create'

    // determine if the asset should be added to the store or not
    if (assetStore.assetExists(asset)) {
      assetStore.deleteTempUpload(uploadInfo);
    } else {
      assetStore.saveUploadedAsset(new Bucket(bucketName), uploadInfo);
      asset.checksum = uploadInfo.getChecksum();
      asset.size = uploadInfo.getSize();
    }

    asset.urls = REPROXY_URL_JOINER.join(assetStore.getRedirectURLs(asset));

    // add a record to the DB
    hsqlService.insertAsset(asset); // TODO: deal with 0 return values

    return new ResponseEntity<>(assetToJsonString(asset, false), status);
  }

}
