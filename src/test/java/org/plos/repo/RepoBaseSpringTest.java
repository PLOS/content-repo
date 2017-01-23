/*
 * Copyright (c) 2017 Public Library of Science
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

package org.plos.repo;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.RepoObject;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.RepoService;
import org.plos.repo.service.SqlService;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.inject.Inject;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestSpringConfig.class)
public abstract class RepoBaseSpringTest {

  @Inject
  protected RepoService repoService;

  @Inject
  protected ObjectStore objectStore;

  @Inject
  protected SqlService sqlService;

  @Inject
  protected DataSource dataSource;

  /**
   * Clean de data base before to run each test
   * @throws Exception
   */
  @Before
  public void clearData() throws Exception {
    try(Connection connection = dataSource.getConnection();
        Statement st = connection.createStatement();) {

      sqlService.getConnection();
      List<RepoObject> repoObjectList = sqlService.listObjects(null, null, null, true, true, null);
      for (RepoObject repoObject : repoObjectList) {
        objectStore.deleteObject(repoObject);
      }

      List<Bucket> bucketList = sqlService.listBuckets();
      for (Bucket bucket : bucketList) {
        objectStore.deleteBucket(bucket);
      }

      st.executeUpdate("delete from collectionObject");
      st.executeUpdate("delete from collections");
      st.executeUpdate("delete from objects");
      st.executeUpdate("delete from buckets");
      st.executeUpdate("delete from audit");
    }
  }
}
