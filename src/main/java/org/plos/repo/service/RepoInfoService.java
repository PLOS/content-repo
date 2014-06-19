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

package org.plos.repo.service;

import org.plos.repo.models.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.io.InputStream;
import java.lang.Object;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class RepoInfoService {

  private static final Logger log = LoggerFactory.getLogger(RepoInfoService.class);

  @Inject
  private ObjectStore objectStore;

  @Inject
  private SqlService sqlService;

  @Inject
  private RepoService repoService;

  private String projectVersion = "unknown";

  private Date startTime;

  private AtomicLong readCount = new AtomicLong(0);

  private AtomicLong writeCount = new AtomicLong(0);

  @PostConstruct
  public void init() {

    startTime = new Date();

    try (InputStream is = getClass().getResourceAsStream("/version.properties")) {
      Properties properties = new Properties();
      properties.load(is);
      projectVersion = properties.get("version") + " (" + properties.get("buildDate") + ")";
    } catch (Exception e) {
      log.error("Error fetching project version", e);
    }
  }

  public void incrementReadCount() {
    readCount.incrementAndGet();
  }

  public void incrementWriteCount() {
    writeCount.incrementAndGet();
  }

  public Map<String, String> getConfig() {
    Map<String, String> infos = new HashMap<>();
    infos.put("version", projectVersion);
    infos.put("objectStoreBackend", objectStore.getClass().toString());
    infos.put("sqlServiceBackend", sqlService.getClass().toString());
    return infos;
  }

  public int countObjects(boolean includeDeleted, String bucketName) throws RepoException {

    try {

      sqlService.getConnection();

      if (bucketName != null && sqlService.getBucketId(bucketName) == null)
        throw new RepoException(RepoException.Type.ItemNotFound, "Bucket not found");

      return sqlService.objectCount(includeDeleted, bucketName);
    } catch (SQLException e) {
      throw new RepoException(RepoException.Type.ServerError, e);
    } finally {
      try {
        sqlService.releaseConnection();
      } catch (SQLException e) {
        log.error(e.getMessage());
        throw new RepoException(RepoException.Type.ServerError, e);
      }
    }
  }

  public Map<String, Object> bucketInfo(String bucketName) throws RepoException {

    HashMap bucketInfo = new HashMap();
    bucketInfo.put("bucket", bucketName);
    bucketInfo.put("totalObjects", countObjects(true, bucketName));
    bucketInfo.put("activeObjects", countObjects(false, bucketName));

    return bucketInfo;
  }

  public Map<String, Object> getStatus() throws Exception {

    List<Bucket> bucketList = repoService.listBuckets();
    List<Map> bucketsOut = new ArrayList<>();

    for (Bucket bucket : bucketList) {
      bucketsOut.add(bucketInfo(bucket.bucketName));
    }

    Map<String, Object> infos = new HashMap<>();

    infos.put("bucketCount", Integer.toString(bucketList.size()));
    //infos.put("buckets", bucketsOut);
    infos.put("serviceStarted", startTime.toString());
    infos.put("readsSinceStart", readCount.toString());
    infos.put("writesSinceStart", writeCount.toString());
    return infos;
  }

//  public Map<String, String> getSysInfo() throws Exception {
//
//    Map<String, String> infos = new HashMap<>();
//    infos.put("version", projectVersion);
//    infos.put("objects", sqlService.objectCount().toString());
//    infos.put("buckets", Integer.toString(sqlService.listBuckets().size()));
//    infos.put("objectStoreBackend", objectStore.getClass().toString());
//    infos.put("sqlServiceBackend", sqlService.getClass().toString());
//    infos.put("serviceStarted", startTime.toString());
//    infos.put("readsSinceStart", readCount.toString());
//    infos.put("writesSinceStart", writeCount.toString());
//
//    return infos;
//  }
}
