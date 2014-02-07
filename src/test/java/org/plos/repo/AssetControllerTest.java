package org.plos.repo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.plos.repo.models.Asset;
import org.plos.repo.models.Bucket;
import org.plos.repo.service.FileSystemStoreService;
import org.plos.repo.service.HsqlService;
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
import java.security.MessageDigest;
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
public class AssetControllerTest extends AbstractTestNGSpringContextTests {

  @Autowired
  HsqlService hsqlService;

  @Autowired
  FileSystemStoreService fileSystemStoreService;

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

  public static void clearData(HsqlService hsqlService, FileSystemStoreService fileSystemStoreService) {
    List<Asset> assetList = hsqlService.listAllAssets();

    for (Asset asset : assetList) {
      hsqlService.deleteAsset(asset.key, asset.checksum, asset.bucketName, asset.timestamp);
      fileSystemStoreService.deleteAsset(fileSystemStoreService.getAssetLocationString(asset.bucketName, asset.checksum));
    }

    List<Bucket> bucketList = hsqlService.listBuckets();

    for (Bucket bucket : bucketList) {
      hsqlService.deleteBucket(bucket.bucketName);
      fileSystemStoreService.deleteBucket(bucket.bucketName);
    }
  }

  @Test
  public void testControllerCrud() throws Exception {

    clearData(hsqlService, fileSystemStoreService);

    MessageDigest md = MessageDigest.getInstance(FileSystemStoreService.digestAlgorithm);

    this.mockMvc.perform(get("/assets").accept(APPLICATION_JSON_UTF8))
        .andExpect(status().isOk())
        .andExpect(content().contentType("application/json;charset=UTF-8"))
        .andExpect(content().string("[]"));

    this.mockMvc.perform(post("/buckets").accept(APPLICATION_JSON_UTF8)
        .param("name", "testbucketAssets"))
        .andExpect(status().isCreated());


    String testData1 = "test data one goes\nhere.";
    MockMultipartFile file1 = new MockMultipartFile("file", testData1.getBytes());
    String testData1Checksum = FileSystemStoreService.checksumToString(md.digest(testData1.getBytes()));

    String testData2 = "test data two goes\nhere.";
    MockMultipartFile file2 = new MockMultipartFile("file", testData2.getBytes());
    String testData2Checksum = FileSystemStoreService.checksumToString(md.digest(testData2.getBytes()));

    // CREATE

    this.mockMvc.perform(fileUpload("/assets").file(file1).param("newAsset", "true")
        .param("key", "asset1").param("bucketName", "testbucketAssets").param("contentType", "text/plain"))
        .andExpect(status().isCreated());

    this.mockMvc.perform(fileUpload("/assets").file(file1).param("newAsset", "true")
        .param("key", "asset1").param("bucketName", "testbucketAssets").param("contentType", "text/plain"))
        .andExpect(status().isConflict());  // since the key exists

    this.mockMvc.perform(fileUpload("/assets").file(file1).param("newAsset", "true")
        .param("key", "asset2").param("bucketName", "testbucketAssets").param("contentType", "text/something").param("downloadName", "asset2.text"))
        .andDo(print())
        .andExpect(status().isCreated()); // since its a new key

    this.mockMvc.perform(fileUpload("/assets").file(file1).param("newAsset", "true")
        .param("key", "asset1").param("bucketName", "testbucketAssets2").param("contentType", "text/plain"))
        .andExpect(status().isInsufficientStorage()); // since the bucket does not exist

    this.mockMvc.perform(fileUpload("/assets").file(new MockMultipartFile("file", "".getBytes())).param("newAsset", "true")
        .param("key", "funky&?#key").param("bucketName", "testbucketAssets").param("contentType", "text/plain"))
        .andExpect(status().isCreated()); // since we accept empty files


    // READ

    this.mockMvc.perform(get("/assets/testbucketAssets/").param("key", "asset1"))
        .andExpect(header().string("Content-Type", "text/plain"))
        .andExpect(header().string("Content-Disposition", "inline; filename=asset1"))
        .andExpect(content().string(testData1))
        .andExpect(status().isFound());  // file name assigned by key

    this.mockMvc.perform(get("/assets/testbucketAssets/").param("key", "asset1").param("checksum", testData1Checksum))
        .andExpect(header().string("Content-Type", "text/plain"))
        .andExpect(header().string("Content-Disposition", "inline; filename=asset1"))
        .andExpect(content().string(testData1))
        .andExpect(status().isFound());  // version accessible

    this.mockMvc.perform(get("/assets/testbucketAssets/").param("key", "asset1").param("checksum", "abc"))
        .andExpect(status().isNotFound());  // version should not exist

    this.mockMvc.perform(get("/assets/testbucketAssets/").param("key", "asset2"))
        .andExpect(header().string("Content-Type", "text/something"))
        .andExpect(header().string("Content-Disposition", "inline; filename=asset2.text"))
        .andExpect(content().string(testData1));  // file name assigned by user

    this.mockMvc.perform(get("/assets/testbucketAssets/").param("key", "funky&?#key"))
        .andExpect(header().string("Content-Type", "text/plain"))
        .andExpect(header().string("Content-Disposition", "inline; filename=content"))
        .andExpect(content().string(""));  // file name can not be set by key or user

    this.mockMvc.perform(get("/assets/testbucketAssets/").param("key", "nonAsset"))
        .andExpect(status().isNotFound());  // asset should not exist


    // UPDATE

    this.mockMvc.perform(fileUpload("/assets").file(file2).param("newAsset", "false")
        .param("key", "asset3").param("bucketName", "testbucketAssets").param("contentType", "text/something").param("downloadName", "asset2.text"))
        .andExpect(status().isNotAcceptable()); // since key does not exist

    this.mockMvc.perform(fileUpload("/assets").file(file2).param("newAsset", "false")
        .param("key", "asset2").param("bucketName", "testbucketAssets").param("contentType", "text/something").param("downloadName", "asset2.text"))
        .andExpect(status().isOk()); // since its a new version


    // LIST

    String responseString = this.mockMvc.perform(get("/assets/testbucketAssets/").param("key", "asset2").param("fetchMetadata", "true"))
        .andExpect(header().string("Content-Type", "application/json"))
        .andReturn().getResponse().getContentAsString();  // file name assigned by user

    Gson gson = new Gson();
    JsonObject jsonObject = gson.fromJson(responseString, JsonElement.class).getAsJsonObject();
    JsonArray jsonArray = jsonObject.getAsJsonArray("versions");

    assert(jsonArray.size() == 2);  // since there should be two versions of this asset


    // DELETE

//    this.mockMvc.perform(delete("/assets/testbucketAssets").param("key", "asset2").param("checksum", testData2Checksum)).andExpect(status().isOk());

//    this.mockMvc.perform(delete("/assets/testbucketAssets").param("key", "asset3").param("checksum", testData2Checksum)).andExpect(status().isNotFound());

  }

}
