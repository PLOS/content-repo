package org.plos.repo.rest;

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
  public @ResponseBody Boolean create(@RequestParam String name)
      throws Exception {
    // TODO: sanitize bucket name to be filesystem acceptable
    return hsqlService.insertBucket(name);
  }
}
