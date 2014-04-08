package org.plos.repo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.plos.repo.models.Bucket;
import org.plos.repo.models.Object;
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
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import javax.naming.NamingException;
import java.nio.charset.Charset;
import java.util.List;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.fileUpload;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebAppConfiguration
@EnableWebMvc
@ContextConfiguration(locations = {"classpath:app-servlet-context.xml"})
public class ObjectControllerTest extends AbstractTestNGSpringContextTests {

  @Autowired
  private SqlService sqlService;

  @Autowired
  private ObjectStore objectStore;

  @Autowired
  private WebApplicationContext wac;

  private MockMvc mockMvc;

  private ObjectMapper jsonObjectMapper = new ObjectMapper();

  public static final MediaType APPLICATION_JSON_UTF8 = new MediaType(MediaType.APPLICATION_JSON.getType(), MediaType.APPLICATION_JSON.getSubtype(), Charset.forName("utf8"));

  // set values before application context is loaded
  static {
    System.setProperty("configFile", "test.properties");
  }

  @BeforeSuite
  private static void injectContextDB() throws NamingException {
    BucketControllerTest.injectContextDB();
  }

  @BeforeClass
  private void setup() {
    this.mockMvc = MockMvcBuilders.webAppContextSetup(this.wac).build();
    jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
  }

  public static void clearData(SqlService sqlService, ObjectStore objectStore) {
    List<org.plos.repo.models.Object> objectList = sqlService.listAllObject();

    for (Object object : objectList) {
      //sqlService.markObjectDeleted(object.key, object.checksum, object.bucketName, object.versionNumber);
      int delD = sqlService.deleteObject(object);
      boolean delS = objectStore.deleteObject(object);
    }

    List<Bucket> bucketList = sqlService.listBuckets();

    for (Bucket bucket : bucketList) {
      sqlService.deleteBucket(bucket.bucketName);
      objectStore.deleteBucket(bucket);
    }
  }

  @Test
  public void testControllerCrud() throws Exception {

    Gson gson = new Gson();

    String bucketName = "plos-objstoreunittest-bucket1";

    clearData(sqlService, objectStore);

    this.mockMvc.perform(get("/objects").accept(APPLICATION_JSON_UTF8))
        .andExpect(status().isOk())
        .andExpect(content().contentType("application/json;charset=UTF-8"))
        .andExpect(content().string("[]"));

    this.mockMvc.perform(post("/buckets").accept(APPLICATION_JSON_UTF8)
        .param("name", bucketName))
        .andExpect(status().isCreated());


    String testData1 = "test data one goes\nhere.";
    MockMultipartFile file1 = new MockMultipartFile("file", testData1.getBytes());

    String testData2 = "test data two goes\nhere.";
    MockMultipartFile file2 = new MockMultipartFile("file", testData2.getBytes());

    // CREATE

    this.mockMvc.perform(fileUpload("/objects").file(file1).param("create", "new")
        .param("key", "object1").param("bucketName", bucketName).param("contentType", "text/plain"))
        .andExpect(status().isCreated());

//    JsonObject jsonObject = gson.fromJson(responseString, JsonElement.class).getAsJsonObject();
//    Long object1Timestamp = jsonObject.get("timestamp_unixnano").getAsLong();

    this.mockMvc.perform(fileUpload("/objects").file(file1).param("create", "new")
        .param("key", "object1").param("bucketName", bucketName).param("contentType", "text/plain"))
        .andExpect(status().isConflict());  // since the key exists

    this.mockMvc.perform(fileUpload("/objects").file(file1).param("create", "new")
        .param("key", "object2").param("bucketName", bucketName).param("contentType", "text/something").param("downloadName", "object2.text"))
        .andExpect(status().isCreated()); // since its a new key

    this.mockMvc.perform(fileUpload("/objects").file(file1).param("create", "new")
        .param("key", "object1").param("bucketName", "testbucketObjects2").param("contentType", "text/plain"))
        .andExpect(status().isInsufficientStorage()); // since the bucket does not exist

    this.mockMvc.perform(fileUpload("/objects").file(new MockMultipartFile("file", "".getBytes())).param("create", "new")
        .param("key", "funky&?#key").param("bucketName", bucketName).param("contentType", "text/plain"))
        .andExpect(status().isCreated()); // since we accept empty files


    // TODO: create the same object in two buckets, and make sure deleting one does not delete the other


    // READ

    this.mockMvc.perform(get("/objects/" + bucketName).param("key", "object1"))
        .andExpect(header().string("Content-Type", "text/plain"))
        .andExpect(header().string("Content-Disposition", "inline; filename=object1"))
        .andExpect(content().string(testData1))
        .andExpect(status().isOk());  // file name assigned by key

    this.mockMvc.perform(get("/objects/" + bucketName).param("key", "object1").param("version", "0"))
        .andExpect(header().string("Content-Type", "text/plain"))
        .andExpect(header().string("Content-Disposition", "inline; filename=object1"))
        .andExpect(content().string(testData1))
        .andExpect(status().isOk());  // version accessible

    this.mockMvc.perform(get("/objects/" + bucketName).param("key", "object1").param("version", "10"))
        .andExpect(status().isNotFound());  // version should not exist

    this.mockMvc.perform(get("/objects/" + bucketName).param("key", "object2"))
        .andExpect(header().string("Content-Type", "text/something"))
        .andExpect(header().string("Content-Disposition", "inline; filename=object2.text"))
        .andExpect(content().string(testData1));  // file name assigned by user

    this.mockMvc.perform(get("/objects/" + bucketName).param("key", "funky&?#key"))
        .andExpect(header().string("Content-Type", "text/plain"))
        .andExpect(header().string("Content-Disposition", "inline; filename=content"))
        .andExpect(content().string(""));  // file name can not be set by key or user

    this.mockMvc.perform(get("/objects/" + bucketName).param("key", "notObject"))
        .andExpect(status().isNotFound());  // object should not exist


    // READ REPROXY

    if (objectStore.hasXReproxy()){
      this.mockMvc.perform(get("/objects/" + bucketName).param("key", "object1").param("version", "0").header("X-Proxy-Capabilities", "reproxy-file"))
          .andDo(print())
          .andExpect(status().isOk())
          .andExpect(content().string(""));
    }


    // UPDATE

    this.mockMvc.perform(fileUpload("/objects").file(file2).param("create", "version")
        .param("key", "object3").param("bucketName", bucketName).param("contentType", "text/something").param("downloadName", "object2.text"))
        .andExpect(status().isNotAcceptable()); // since key does not exist

    this.mockMvc.perform(fileUpload("/objects").file(file2).param("create", "version")
        .param("key", "object2").param("bucketName", bucketName).param("contentType", "text/something").param("downloadName", "object2.text"))
        .andExpect(status().isOk()); // since its a new version


    // AUTO CREATE

    this.mockMvc.perform(fileUpload("/objects").file(file2).param("create", "auto")
        .param("key", "object2").param("bucketName", bucketName).param("contentType", "text/something").param("downloadName", "object2.text"))
        .andExpect(status().isOk()); // since its a new version

    this.mockMvc.perform(fileUpload("/objects").file(file2).param("create", "badrequest")
        .param("key", "object2").param("bucketName", bucketName).param("contentType", "text/something").param("downloadName", "object2.text"))
        .andExpect(status().isBadRequest()); // since creation flag is bad

    this.mockMvc.perform(fileUpload("/objects").file(file2).param("create", "auto")
        .param("key", "object4").param("bucketName", bucketName))
        .andExpect(status().isCreated());  // since it is a new object


    // LIST

    String responseString = this.mockMvc.perform(get("/objects/" + bucketName).param("key", "object2").param("fetchMetadata", "true"))
        .andExpect(header().string("Content-Type", "application/json"))
        .andReturn().getResponse().getContentAsString();  // file name assigned by user

    JsonObject jsonObject = gson.fromJson(responseString, JsonElement.class).getAsJsonObject();
    JsonArray jsonArray = jsonObject.getAsJsonArray("versions");

    assert(jsonArray.size() == 3);


    // DELETE

//    this.mockMvc.perform(delete("/objects/" + bucketName).param("key", "object1").param("version", "0"))
//        .andExpect(status().isOk());

    // TODO: tests to add
    //   object deduplication
    //   check url redirect resolve order (db vs filestore)



    // clean up
//    clearData(sqlService, objectStore);

  }

}
