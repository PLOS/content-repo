package org.plos.repo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import org.plos.repo.service.ObjectStore;
import org.plos.repo.service.SqlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.charset.Charset;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.fileUpload;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebAppConfiguration
@EnableWebMvc
@ContextConfiguration(locations = {"classpath:app-servlet-context.xml"})
public class BucketControllerTest extends AbstractTestNGSpringContextTests {

  @Autowired
  SqlService sqlService;

  @Autowired
  ObjectStore objectStore;

  @Autowired
  private WebApplicationContext wac;

  private MockMvc mockMvc;

  private ObjectMapper jsonObjectMapper = new ObjectMapper();

  public static final MediaType APPLICATION_JSON_UTF8 = new MediaType(MediaType.APPLICATION_JSON.getType(), MediaType.APPLICATION_JSON.getSubtype(), Charset.forName("utf8"));

  // set values before application context is loaded
  static {
    System.setProperty("configFile", "test.properties");
  }

  @BeforeClass
  private void setup() {
    this.mockMvc = MockMvcBuilders.webAppContextSetup(this.wac).build();
    jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
  }


  @Test
  public void testControllerCrud() throws Exception {

    ObjectControllerTest.clearData(sqlService, objectStore);


    // CREATE

    this.mockMvc.perform(get("/buckets").accept(APPLICATION_JSON_UTF8))
        .andExpect(status().isOk())
        .andExpect(content().contentType("application/json;charset=UTF-8"))
        .andExpect(content().string("[]"));

    this.mockMvc.perform(post("/buckets").accept(APPLICATION_JSON_UTF8)
        .param("name", "plos-bucketunittest-bucket1"))
        .andExpect(status().isCreated());

    this.mockMvc.perform(post("/buckets").accept(APPLICATION_JSON_UTF8)
        .param("name", "plos-bucketunittest-bucket1"))
        .andExpect(status().isNoContent());

    this.mockMvc.perform(post("/buckets").accept(APPLICATION_JSON_UTF8)
        .param("name", "plos-bucketunittest-bucket2").param("id", "5"))
        .andExpect(status().isCreated());

    this.mockMvc.perform(post("/buckets").accept(APPLICATION_JSON_UTF8)
        .param("name", "plos-bucketunittest-bad?&name"))
        .andExpect(status().isPreconditionFailed());


    // LIST

    String responseString = this.mockMvc.perform(get("/buckets"))
//        .andExpect(header().string("Content-Type", "application/json"))
        .andReturn().getResponse().getContentAsString();  // file name assigned by user

    Gson gson = new Gson();
    JsonArray jsonArray = gson.fromJson(responseString, JsonElement.class).getAsJsonArray();

    assert(jsonArray.size() == 2);


    // DELETE

    this.mockMvc.perform(delete("/buckets/plos-bucketunittest-bucket1")).andExpect(status().isOk());

    this.mockMvc.perform(fileUpload("/objects").file(new MockMultipartFile("file", "test".getBytes())).param("create", "new")
        .param("key", "object1").param("bucketName", "plos-bucketunittest-bucket2"))
        .andDo(print())
        .andExpect(status().isCreated());

    this.mockMvc.perform(delete("/buckets/plos-bucketunittest-bucket2")).andExpect(status().isNotModified());

    this.mockMvc.perform(delete("/buckets/plos-bucketunittest-bucket3")).andExpect(status().isNotFound());


    // clean up

    ObjectControllerTest.clearData(sqlService, objectStore);
  }

}
