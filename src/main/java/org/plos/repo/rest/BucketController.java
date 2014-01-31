package org.plos.repo.rest;

import org.plos.repo.service.FileSystemStoreService;
import org.plos.repo.service.HsqlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;
import java.util.List;

@Controller
@RequestMapping("/buckets")
public class BucketController {

  @Autowired
  private FileSystemStoreService fileSystemStoreService;

  @Autowired
  private HsqlService hsqlService;

  @RequestMapping(method = RequestMethod.GET)
  public @ResponseBody
  List<HashMap<String, Object>> list() throws Exception {
    return hsqlService.listBuckets();
  }

  @RequestMapping(value = "/count", method = RequestMethod.GET)
  public @ResponseBody
  Integer count() throws Exception {
    return hsqlService.listBuckets().size();
  }

  @RequestMapping(method = RequestMethod.POST)
  public @ResponseBody String create(@RequestParam String name,
                                     @RequestParam(required = false) Integer id)
      throws Exception {

    if (!fileSystemStoreService.createBucket(name))
      return "Unable to create bucket " + name;

    if (!hsqlService.insertBucket(name, id)) {
      fileSystemStoreService.deleteBucket(name);
      return "Unable to create bucket " + name;
    }

    return "Created bucket " + name;
  }
}
