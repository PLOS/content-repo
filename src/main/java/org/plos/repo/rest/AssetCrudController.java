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

import org.apache.commons.io.IOUtils;
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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/assets")
public class AssetCrudController {

  private static final Logger log = LoggerFactory.getLogger(AssetCrudController.class);

  @Autowired
  private FileSystemStoreService fileSystemStoreService;

  @Autowired
  private HsqlService hsqlService;



  // TODO: figure out what is meant by an "UPDATE"




  @RequestMapping(method=RequestMethod.GET)
  public @ResponseBody List<HashMap<String, Object>> list() throws Exception {
    return hsqlService.listAssets();
  }

  @RequestMapping(value="{bucketName}/{key}/{checksum}", method=RequestMethod.GET)
  public @ResponseBody
  void readVersion(@PathVariable String bucketName,
                   @PathVariable String key,
                   @PathVariable String checksum,
                   HttpServletResponse response) throws Exception {

    if (hsqlService.getAsset(bucketName, key, checksum) != null) {
      try {
        InputStream is = new FileInputStream(fileSystemStoreService.getAssetLocationString(bucketName, checksum));
        IOUtils.copy(is, response.getOutputStream());
        response.flushBuffer();
      } catch (IOException ex) {
        log.info("Error writing file to output stream.");
      }
    }
  }

  @RequestMapping(value="{bucketName}/{key:.*}", method=RequestMethod.GET)
  public @ResponseBody
  void readLatest(@PathVariable String bucketName,
                  @PathVariable String key,
                  HttpServletResponse response) throws Exception {

    log.info("searching for key " + key);

    HashMap<String, Object> asset = hsqlService.getAsset(bucketName, key, null);

    if (asset != null) {

      String checksum = asset.get("CHECKSUM").toString();   // TODO: understand capitalization
      try {
        InputStream is = new FileInputStream(fileSystemStoreService.getAssetLocationString(bucketName, checksum));
        IOUtils.copy(is, response.getOutputStream());
        response.flushBuffer();
      } catch (IOException ex) {
        log.info("Error writing file to output stream.");
      }
    }
  }


//  @RequestMapping(value="{bucketName}/{key}/{checksum}", method=RequestMethod.GET)
//  public @ResponseBody
//  FileSystemResource readVersion(@PathVariable String bucketName,
//                                          @PathVariable String key,
//                                          @PathVariable String checksum) throws Exception {
//
//      // TODO: set content type so it gets downloaded unstead of turned to text output
//    if (hsqlService.getAsset(bucketName, key, checksum) != null)
//      return new FileSystemResource(fileSystemStoreService.getAssetLocationString(bucketName, checksum));
//
//    return null;
//  }

  @RequestMapping(value="{bucketName}/{key}/{checksum}", method=RequestMethod.DELETE)
  public @ResponseBody String delete(@PathVariable String bucketName,
                                     @PathVariable String key,
                                     @PathVariable String checksum) throws Exception {

    if (hsqlService.removeAsset(key, checksum, bucketName) == 0)
      return "Error: can not find asset in database";

    if (!fileSystemStoreService.deleteFile(fileSystemStoreService.getAssetLocationString(bucketName, checksum)))
      return "Error: there was a problem deleting the asset from the filesystem";

    return checksum + " deleted";
  }

  @RequestMapping(method=RequestMethod.POST)
  public @ResponseBody String create(@RequestParam String key,
                                     @RequestParam String bucketName,
                                     @RequestParam MultipartFile file)
  throws Exception {

    Integer bucketId = hsqlService.getBucketId(bucketName);

    if (file.isEmpty())
      return "Error: file is empty";

    if (bucketId == null)
      return "Error: can not find bucket " + bucketName;

    Map.Entry<String, String> uploadResult = fileSystemStoreService.uploadFile(file);  // TODO: make sure this is successful
    String tempFileLocation = uploadResult.getKey();
    String checksum = uploadResult.getValue();

    if (hsqlService.assetExists(key, checksum, bucketId)) {
      fileSystemStoreService.deleteFile(tempFileLocation);
      log.info("skipping insert since asset already exists");
    } else {
      fileSystemStoreService.saveUploaded(bucketName, checksum, tempFileLocation);
      log.info("insert: " + hsqlService.insertAsset(key, checksum, bucketId, new Date()));
    }

    return checksum;
  }

  @RequestMapping(value = "/count", method = RequestMethod.GET)
  public @ResponseBody Integer count() throws Exception {
    return hsqlService.assetCount();
  }

}
