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

package org.plos.repo;

import org.junit.runner.RunWith;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.RepoCollection;
import org.plos.repo.models.RepoObject;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.RepoService;
import org.plos.repo.service.SqlService;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.inject.Inject;
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

  public static void clearData(ObjectStore objectStore, SqlService sqlService) throws Exception {

    sqlService.getConnection();

    // remove collections from DB
    List<RepoCollection> repoCollectionList = sqlService.listCollections(null,null,null,true,null);

    for (RepoCollection repoCollection : repoCollectionList) {

      if (sqlService.deleteCollection(repoCollection) == 0)
        throw new Exception("Collection not deleted in DB");

    }

    //remove objects from DB

    List<RepoObject> repoObjectList = sqlService.listObjects(null, null, null, true, true, null);

    for (RepoObject repoObject : repoObjectList) {

      if (sqlService.deleteObject(repoObject) == 0)
        throw new Exception("Object not deleted in DB");

      objectStore.deleteObject(repoObject);
    }

    // remove buckets from DB

    List<Bucket> bucketList = sqlService.listBuckets();

    for (Bucket bucket : bucketList) {
      sqlService.deleteBucket(bucket.getBucketName());
      objectStore.deleteBucket(bucket);
    }
    
    // TODO: assert both refs are empty

    sqlService.transactionCommit();
  }

}
