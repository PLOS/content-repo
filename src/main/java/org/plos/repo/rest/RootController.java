package org.plos.repo.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

@Path("/")
public class RootController {


  // TODO: add some swagger

  @GET
  public String index() {

    // TODO: display spirit animal

    return "<h1>PLoS Content Repository REST API</h1>";
  }

  // TODO: write this function
//  @GET
//  @Path("info")
//  public Response info() {
//
//    final HashMap<String, String> vars = new HashMap<>();
//    vars.put("version", ...);
//    // TODO: add database etc., add object count
//
//    return Response.status(Response.Status.OK).entity(
//        new GenericEntity<Map<String, String>>(vars){}).build();
//  }
}
