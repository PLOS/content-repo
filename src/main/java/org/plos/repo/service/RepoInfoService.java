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

import org.plos.repo.models.Bucket;
import org.plos.repo.models.ServiceConfigInfo;
import org.plos.repo.models.ServiceStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
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


  private void sqlReleaseConnection() throws RepoException {

    try {
      sqlService.releaseConnection();
    } catch (SQLException e) {
      throw new RepoException(e);
    }

  }

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

  public ServiceConfigInfo getConfig() {

    ServiceConfigInfo config = new ServiceConfigInfo();
    config.version = projectVersion;
    config.objectStoreBackend = objectStore.getClass().toString();
    config.sqlServiceBackend = sqlService.getClass().toString();
    config.hasXReproxy = objectStore.hasXReproxy();
    return config;
  }

  public ServiceStatus getStatus() throws RepoException {

    ServiceStatus status = new ServiceStatus();

    List<Bucket> bucketList = repoService.listBuckets();
    status.bucketCount = Integer.toString(bucketList.size());

    status.serviceStarted = startTime.toString();
    status.readsSinceStart = readCount.toString();
    status.writesSinceStart = writeCount.toString();

    return status;
  }

  public Bucket bucketInfo(String bucketName) throws RepoException {

    try {
      sqlService.getConnection();

      Bucket bucket = sqlService.getBucket(bucketName);

      if (bucket == null)
        throw new RepoException(RepoException.Type.BucketNotFound);

      bucket.totalObjects = sqlService.objectCount(true, bucketName);
      bucket.activeObjects = sqlService.objectCount(false, bucketName);

      return bucket;

    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {
      sqlReleaseConnection();
    }
  }

}
