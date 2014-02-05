package org.plos.repo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.List;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
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

  private void clearData() {
    List<Asset> assetList = hsqlService.listAssets();

    for (Asset asset : assetList) {
      hsqlService.deleteAsset(asset.key, asset.checksum, asset.bucketName);
      fileSystemStoreService.deleteAsset(fileSystemStoreService.getAssetLocationString(asset.bucketName, asset.checksum, asset.timestamp));
    }

    List<Bucket> bucketList = hsqlService.listBuckets();

    for (Bucket bucket : bucketList) {
      hsqlService.deleteBucket(bucket.bucketName);
      fileSystemStoreService.deleteBucket(bucket.bucketName);
    }
  }

  @Test
  public void testControllerCrud() throws Exception {

    clearData();

    this.mockMvc.perform(get("/assets").accept(APPLICATION_JSON_UTF8))
        .andExpect(status().isOk())
        .andExpect(content().contentType("application/json;charset=UTF-8"))
        .andExpect(content().string("[]"));

    this.mockMvc.perform(post("/buckets").accept(APPLICATION_JSON_UTF8)
        .param("name", "testbucketAssets"))
        .andDo(print())
        .andExpect(status().isCreated());

    byte[] testData1 = "test data one goes\nhere.".getBytes();
    MockMultipartFile file = new MockMultipartFile("someFileName.txt", testData1);

//    this.mockMvc.perform(post("/assets").accept(APPLICATION_JSON_UTF8)
//        .param("key", "asset1").param("bucketName").param("contentType", "text/plain"))
//        .andExpect(status().isOk());

    // TODO: figure out upload while posting params

  }

}
