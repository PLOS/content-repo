package org.plos.repo.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.AbstractMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

public class FileSystemStoreService implements AssetStore {

  private static final Logger log = LoggerFactory.getLogger(FileSystemStoreService.class);

  public static final String digestAlgorithm = "SHA-1";

  private String data_dir;

  @Required
  public void setPreferences(Preferences preferences) {
    data_dir = preferences.getDataDirectory();
  }

  public static boolean isValidFileName(String name) {
    return !Pattern.compile("[^-_.A-Za-z0-9]").matcher(name).find();
  }

  private String getBucketLocationString(String bucketName) {
    return data_dir + "/" + bucketName + "/";
  }

  public String getAssetLocationString(String bucketName, String checksum) {
    return getBucketLocationString(bucketName) + checksum;
  }

  public static String checksumToString(byte[] checksum) {

    StringBuilder sb = new StringBuilder();

    for (byte b : checksum)
      sb.append(Integer.toHexString((b & 0xFF) | 0x100).substring(1,3));

    return sb.toString();
  }

  public boolean createBucket(String bucketName) {

    File dir = new File(getBucketLocationString(bucketName));
    boolean result = dir.mkdir();

    if (!result)
      log.debug("Error while creating bucket. Directory was not able to be created : " + getBucketLocationString(bucketName));

    return result;
  }

  public boolean deleteBucket(String bucketName) {
    File dir = new File(getBucketLocationString(bucketName));
    return dir.delete();
  }

  public boolean saveUploadedAsset(String bucketName, String checksum, String tempFileLocation)
  throws Exception {
    File tempFile = new File(tempFileLocation);
    return tempFile.renameTo(new File(getAssetLocationString(bucketName, checksum)));
  }

  public boolean deleteAsset(String fileLocation) {
    return new File(fileLocation).delete();
  }

  public Map.Entry<String, String> uploadTempAsset(MultipartFile file) throws Exception {
    String tempFileLocation = data_dir + "/" + UUID.randomUUID().toString() + ".tmp";

    ReadableByteChannel in = Channels.newChannel(file.getInputStream());
    MessageDigest digest = MessageDigest.getInstance(digestAlgorithm);
    WritableByteChannel out = Channels.newChannel(
        new DigestOutputStream(new FileOutputStream(tempFileLocation), digest));
    ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);

    while (in.read(buffer) != -1) {
      buffer.flip();
      out.write(buffer);
      buffer.clear();
    }

    String checksum = checksumToString(digest.digest());

    return new AbstractMap.SimpleEntry<>(tempFileLocation, checksum);
  }

}
