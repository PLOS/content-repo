package org.plos.repo.rest;

import org.plos.repo.models.Bucket;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.SqlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.util.List;

//@Controller
//@RequestMapping("/buckets")
@Component
@Path("/buckets")
public class BucketController {

  @Autowired
  private ObjectStore objectStore;

  @Autowired
  private SqlService sqlService;

  @GET
//  @Produces({"application/json"})
  public List<Bucket> list() throws Exception {
    return sqlService.listBuckets();
//    return Response.status(200).entity(sqlService.listBuckets()).build();
  }

  @GET
  @Path("response")
  public Response listResponse() throws Exception {
    return Response.status(200).entity(sqlService.listBuckets()).build();
  }

/*
  @RequestMapping(method = RequestMethod.POST)
  public ResponseEntity<String> create(@RequestParam String name,
                                       @RequestParam(required = false) Integer id) {

    if (sqlService.getBucketId(name) != null)
      return new ResponseEntity<>("Bucket already exists", HttpStatus.NO_CONTENT);

    if (!ObjectStore.isValidFileName(name))
      return new ResponseEntity<>("Unable to create bucket. Name contains illegal characters: " + name, HttpStatus.PRECONDITION_FAILED);

    Bucket bucket = new Bucket(name, id);

    if (!objectStore.createBucket(bucket))
      return new ResponseEntity<>("Unable to create bucket " + name, HttpStatus.CONFLICT);

    if (!sqlService.insertBucket(bucket)) {
      objectStore.deleteBucket(bucket);
      return new ResponseEntity<>("Unable to create bucket " + name, HttpStatus.CONFLICT);
    }

    return new ResponseEntity<>("Created bucket " + name, HttpStatus.CREATED);
  }

  @RequestMapping(value="{name}", method = RequestMethod.DELETE)
  public ResponseEntity<String> delete(@PathVariable String name) {

    if (sqlService.getBucketId(name) == null)
      return new ResponseEntity<>("Cannot delete bucket. Bucket not found", HttpStatus.NOT_FOUND);

    if (sqlService.listObjectsInBucket(name).size() != 0)
      return new ResponseEntity<>("Cannot delete bucket " + name + " because it contains objects.", HttpStatus.NOT_MODIFIED);

    Bucket bucket = new Bucket(name);

    if (!objectStore.deleteBucket(bucket))
      return new ResponseEntity<>("There was a problem removing the bucket", HttpStatus.NOT_MODIFIED);

    if (sqlService.deleteBucket(name) > 0)
      return new ResponseEntity<>("Bucket " + name + " deleted.", HttpStatus.OK);

    return new ResponseEntity<>("No buckets deleted.", HttpStatus.NOT_MODIFIED);

  }
*/
}
