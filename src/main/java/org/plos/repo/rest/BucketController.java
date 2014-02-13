package org.plos.repo.rest;

import org.plos.repo.models.Bucket;
import org.plos.repo.service.AssetStore;
import org.plos.repo.service.HsqlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
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
                                       @RequestParam(required = false) Integer id) {

    if (hsqlService.getBucketId(name) != null)
      return new ResponseEntity<>("Bucket already exists", HttpStatus.NOT_EXTENDED);

    if (!AssetStore.isValidFileName(name))
      return new ResponseEntity<>("Unable to create bucket. Name contains illegal file path characters: " + name, HttpStatus.PRECONDITION_FAILED);

    if (!assetStore.createBucket(name))
      return new ResponseEntity<>("Unable to create bucket " + name, HttpStatus.CONFLICT);

    if (!hsqlService.insertBucket(name, id)) {
      assetStore.deleteBucket(name);
      return new ResponseEntity<>("Unable to create bucket " + name, HttpStatus.CONFLICT);
    }

    return new ResponseEntity<>("Created bucket " + name, HttpStatus.CREATED);
  }

  @RequestMapping(value="{name}", method = RequestMethod.DELETE)
  public ResponseEntity<String> delete(@PathVariable String name) {

    if (hsqlService.getBucketId(name) == null)
      return new ResponseEntity<>("Cannot delete bucket. Bucket not found", HttpStatus.NOT_FOUND);

    if (hsqlService.listAssetsInBucket(name).size() != 0)
      return new ResponseEntity<>("Cannot delete bucket " + name + " because it contains assets.", HttpStatus.NOT_MODIFIED);

    if (!assetStore.deleteBucket(name))
      return new ResponseEntity<>("There was a problem removing the bucket", HttpStatus.NOT_MODIFIED);

    if (hsqlService.deleteBucket(name) > 0)
      return new ResponseEntity<>("Bucket " + name + " deleted.", HttpStatus.OK);

    return new ResponseEntity<>("No buckets deleted.", HttpStatus.NOT_MODIFIED);

  }

}
