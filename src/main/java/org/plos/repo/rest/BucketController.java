package org.plos.repo.rest;

import org.plos.repo.models.Bucket;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.SqlService;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.Response;
import java.util.List;

//@Component
@Path("/buckets")
public class BucketController {

  @Autowired
  private ObjectStore objectStore;

  @Autowired
  private SqlService sqlService;

  @GET
  public Response list() throws Exception {
    return Response.status(Response.Status.OK).entity(
        new GenericEntity<List<Bucket>>(sqlService.listBuckets()){}).build();
  }

  @POST
  public Response create(@FormParam("name") String name,
                         @FormParam("id") Integer id) {

    if (sqlService.getBucketId(name) != null)
      return Response.status(Response.Status.NO_CONTENT).entity("Bucket already exists").build();

    if (!ObjectStore.isValidFileName(name))
      return Response.status(Response.Status.PRECONDITION_FAILED).entity("Unable to create bucket. Name contains illegal characters: " + name).build();

    Bucket bucket = new Bucket(name, id);

    if (!objectStore.createBucket(bucket))
      return Response.status(Response.Status.CONFLICT).entity("Unable to create bucket " + name).build();

    if (!sqlService.insertBucket(bucket)) {
      objectStore.deleteBucket(bucket);
      return Response.status(Response.Status.CONFLICT).entity("Unable to create bucket " + name).build();
    }

    return Response.status(Response.Status.CREATED).entity("Created bucket " + name).build();
  }

  @DELETE
  @Path("{name}")
  public Response delete(@PathParam("name") String name) {

    if (sqlService.getBucketId(name) == null)
      return Response.status(Response.Status.NOT_FOUND).entity("Cannot delete bucket. Bucket not found.").build();

    if (sqlService.listObjectsInBucket(name).size() != 0)
      return Response.status(Response.Status.NOT_MODIFIED).entity("Cannot delete bucket " + name + " because it contains objects.").build();

    Bucket bucket = new Bucket(name);

    if (!objectStore.deleteBucket(bucket))
      return Response.status(Response.Status.NOT_MODIFIED).entity("There was a problem removing the bucket").build();

    if (sqlService.deleteBucket(name) > 0)
      return Response.status(Response.Status.OK).entity("Bucket " + name + " deleted.").build();

    return Response.status(Response.Status.NOT_MODIFIED).entity("No buckets deleted.").build();

  }

}
