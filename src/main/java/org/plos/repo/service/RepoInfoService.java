/*
 * Copyright (c) 2014-2019 Public Library of Science
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

package org.plos.repo.service;

import org.plos.repo.models.Bucket;
import org.plos.repo.models.ServiceConfigInfo;
import org.plos.repo.models.output.ServiceStatus;
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

    status.bucketCount = bucketList.size();
    status.serviceStarted = startTime.toString();
    status.readsSinceStart = readCount;
    status.writesSinceStart = writeCount;

    return status;
  }

  public Bucket bucketInfo(String bucketName) throws RepoException {
    try {
      sqlService.getReadOnlyConnection();

      Bucket bucket = sqlService.getBucket(bucketName);

      if (bucket == null) {
        throw new RepoException(RepoException.Type.BucketNotFound);
      }

      bucket.setTotalObjects(sqlService.objectCount(true, bucketName));
      bucket.setActiveObjects(sqlService.objectCount(false, bucketName));

      return bucket;
    } catch (SQLException e) {
      throw new RepoException(e);
    } finally {
      sqlReleaseConnection();
    }
  }

}
