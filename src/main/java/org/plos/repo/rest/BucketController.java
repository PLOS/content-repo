package org.plos.repo.rest;

import org.plos.repo.models.Bucket;
import org.plos.repo.service.AssetStore;
import org.plos.repo.service.HsqlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

@Controller
@RequestMapping("/buckets")
public class BucketController {

  @Autowired
  private AssetStore assetStore;

  @Autowired
  private HsqlService hsqlService;

  @RequestMapping(method = RequestMethod.GET)
  public ResponseEntity<List<Bucket>> list() throws Exception {
    return new ResponseEntity<>(hsqlService.listBuckets(), HttpStatus.OK);
  }

  @RequestMapping(method = RequestMethod.POST)
  public ResponseEntity<String> create(@RequestParam String name,
                                       @RequestParam(required = false) Integer id)
      throws Exception {

    if (!assetStore.createBucket(name))
      return new ResponseEntity<>("Unable to create bucket " + name, HttpStatus.NOT_MODIFIED);

    if (!hsqlService.insertBucket(name, id)) {
      assetStore.deleteBucket(name);
      return new ResponseEntity<>("Unable to create bucket " + name, HttpStatus.NOT_MODIFIED);
    }

    return new ResponseEntity<>("Created bucket " + name, HttpStatus.CREATED);
  }

}
