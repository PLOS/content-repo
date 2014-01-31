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
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

public class FileSystemStoreService {

  private static final Logger log = LoggerFactory.getLogger(FileSystemStoreService.class);

  private String data_dir;

  @Required
  public void setPreferences(Preferences preferences) {
    data_dir = preferences.getDataDirectory();
    // TODO: create directory if it does not exist?
  }

  public static boolean isValidFileName(String name) {
    return !Pattern.compile("[^-_.A-Za-z0-9]").matcher(name).find();
  }

  private String getBucketLocationString(String bucketName) {
    return data_dir + "/" + bucketName + "/";
  }

  public String getAssetLocationString(String bucketName, String checksum, Date timestamp) {
    return getBucketLocationString(bucketName) + checksum + "-" + timestamp.getTime() + ".md5";
  }

  private static String checksumToString(byte[] checksum) {

    StringBuilder sb = new StringBuilder();

    for (byte b : checksum)
      sb.append(Integer.toHexString((b & 0xFF) | 0x100).substring(1,3));

    return sb.toString();
  }

  public boolean createBucket(String bucketName) {

    if (!isValidFileName(bucketName)) {
      log.error("Error creating bucket. Name contains illegal file path characters: " + bucketName);
      return false;
    }

    File dir = new File(getBucketLocationString(bucketName));
    boolean result = dir.mkdir();

    if (!result)
      log.error("Error while creating bucket. Directory was not able to be created : " + getBucketLocationString(bucketName));

    return result;
  }

  public boolean deleteBucket(String bucketName) {
    File dir = new File(getBucketLocationString(bucketName));
    return dir.delete();
  }

  public boolean saveUploaded(String bucketName, String checksum, String tempFileLocation, Date timestamp)
  throws Exception {
    File tempFile = new File(tempFileLocation);
    return tempFile.renameTo(new File(getAssetLocationString(bucketName, checksum, timestamp)));
  }

  public boolean deleteFile(String fileLocation) {
    return new File(fileLocation).delete();
  }

  public Map.Entry<String, String> uploadFile(MultipartFile file) throws Exception {
    String tempFileLocation = data_dir + "/" + UUID.randomUUID().toString() + ".md5.tmp";

    ReadableByteChannel in = Channels.newChannel(file.getInputStream());
    MessageDigest md5Digest = MessageDigest.getInstance("MD5");
    WritableByteChannel out = Channels.newChannel(
        new DigestOutputStream(new FileOutputStream(tempFileLocation), md5Digest));
    ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);

    while (in.read(buffer) != -1) {
      buffer.flip();
      out.write(buffer);
      buffer.clear();
    }

    // TODO: check to make sure it does not overwrite existing file, append timestamp for conflict?

    String checksum = checksumToString(md5Digest.digest());

    return new AbstractMap.SimpleEntry<>(tempFileLocation, checksum);
  }

}
