package org.plos.repo.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

public class FileSystemStoreService {

  private static final Logger log = LoggerFactory.getLogger(FileSystemStoreService.class);

  private static final String data_dir = "/tmp";  // TODO: use prefs location


  private static String checksumToString(byte[] checksum) {

    StringBuffer sb = new StringBuffer();

    for (int i = 0; i < checksum.length; ++i) {
      sb.append(Integer.toHexString((checksum[i] & 0xFF) | 0x100).substring(1,3));
    }
    return sb.toString();
  }

  public String getAssetLocationString(String bucketName, String checksum) {
    return data_dir + "/" + bucketName + "/" + checksum + ".md5";
  }

  public boolean saveUploaded(String bucketName, String checksum, String tempFileLocation)
  throws Exception {
    File tempFile = new File(tempFileLocation);
    return tempFile.renameTo(new File(getAssetLocationString(bucketName, checksum)));
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
