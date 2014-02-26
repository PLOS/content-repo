package org.plos.repo.rest;

import org.plos.repo.models.Bucket;
import org.plos.repo.service.ObjectStore;
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
  private ObjectStore objectStore;

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

    if (!ObjectStore.isValidFileName(name))
      return new ResponseEntity<>("Unable to create bucket. Name contains illegal characters: " + name, HttpStatus.PRECONDITION_FAILED);

    Bucket bucket = new Bucket(name, id);

    if (!objectStore.createBucket(bucket))
      return new ResponseEntity<>("Unable to create bucket " + name, HttpStatus.CONFLICT);

    if (!hsqlService.insertBucket(bucket)) {
      objectStore.deleteBucket(bucket);
      return new ResponseEntity<>("Unable to create bucket " + name, HttpStatus.CONFLICT);
    }

    return new ResponseEntity<>("Created bucket " + name, HttpStatus.CREATED);
  }

  @RequestMapping(value="{name}", method = RequestMethod.DELETE)
  public ResponseEntity<String> delete(@PathVariable String name) {

    if (hsqlService.getBucketId(name) == null)
      return new ResponseEntity<>("Cannot delete bucket. Bucket not found", HttpStatus.NOT_FOUND);

    if (hsqlService.listObjectsInBucket(name).size() != 0)
      return new ResponseEntity<>("Cannot delete bucket " + name + " because it contains objects.", HttpStatus.NOT_MODIFIED);

    Bucket bucket = new Bucket(name);

    if (!objectStore.deleteBucket(bucket))
      return new ResponseEntity<>("There was a problem removing the bucket", HttpStatus.NOT_MODIFIED);

    if (hsqlService.deleteBucket(name) > 0)
      return new ResponseEntity<>("Bucket " + name + " deleted.", HttpStatus.OK);

    return new ResponseEntity<>("No buckets deleted.", HttpStatus.NOT_MODIFIED);

  }

}
