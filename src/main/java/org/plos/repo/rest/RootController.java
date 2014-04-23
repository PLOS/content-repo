package org.plos.repo.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

@Path("/")
public class RootController {


  // TODO: add some swagger

  @GET
  public String index() {

    // TODO: display something useful here, like number of objects...

    return "<h1>PLoS Content Repository REST API</h1>";
  }

  // TODO: write the config function
//  @GET
//  @Path("config")
//  public Response config() {
//
//    final HashMap<String, String> vars = new HashMap<>();
//    vars.put("version", ...);
//    // TODO: add database etc.
//
//    return Response.status(Response.Status.OK).entity(
//        new GenericEntity<Map<String, String>>(vars){}).build();
//  }
}
