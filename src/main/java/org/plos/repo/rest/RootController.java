package org.plos.repo.rest;

import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.SqlService;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Path("/")
public class RootController {

  @Inject
  private ObjectStore objectStore;

  @Inject
  private SqlService sqlService;

  @GET
  public String index() {

    // TODO: display spirit animal

    return "<h1>PLoS Content Repository REST API</h1><a href=docs>API doc</a>";
  }

  public String getProjectVersion() {

    // TODO: move this function somewhere else

    try (InputStream is = getClass().getResourceAsStream("/version.properties")) {
      Properties properties = new Properties();
      properties.load(is);
      return properties.get("version") + " (" + properties.get("buildDate") + ")";
    } catch (Exception e) {
      return "unknown";
    }
  }

  // TODO: should we add /hasXReproxy ?

  @GET
  @Path("info")
  public Map info() throws Exception {

    return new HashMap<String, String>(){{
      put("version", getProjectVersion());
      put("objects", sqlService.objectCount().toString());
      put("buckets", Integer.toString(sqlService.listBuckets().size()));
      put("objectStoreBackend", objectStore.getClass().toString());
      put("sqlServiceBackend", sqlService.getClass().toString());
    }};

  }

//  @GET
//  @Path("response")
//  public Response response() throws Exception {
//
//    HashMap<String, String> data = new HashMap<String, String>(){{
//      put("version", getProjectVersion());
//      put("objects", sqlService.objectCount().toString());
//      put("buckets", Integer.toString(sqlService.listBuckets().size()));
//      put("objectStoreBackend", objectStore.getClass().toString());
//      put("sqlServiceBackend", sqlService.getClass().toString());
//    }};
//
//    GenericEntity<Map<String, String>> entity = new GenericEntity<Map<String, String>>(data) {};
//    Response response = Response.ok(entity).build();
//
//    return response;
//  }

}
