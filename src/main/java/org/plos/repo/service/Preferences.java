package org.plos.repo.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Preferences {

  private static Logger log = LoggerFactory.getLogger(Preferences.class);

  private static final String FIELD_DATADIR = "data_directory";

  private static final String MOGILE_TRACKERS = "mogile_trackers";

  private String[] configFiles;

  @Required
  public void setConfigFiles(String[] configFiles) {
    this.configFiles = configFiles;
    log.info("using configs: " + getConfigs());
  }

  public String getDataDirectory() {

    String dir = loadConfigs().getProperty(FIELD_DATADIR);

    File f = new File(dir);
    if (!f.isDirectory())
      f.mkdir();

    return dir;
  }

  public String getHsqldbConnectionString() {
    return "jdbc:hsqldb:" + getDataDirectory() + "/" + HsqlService.fileName;
  }

  public String[] getMogileTrackers() {
    return loadConfigs().getProperty(MOGILE_TRACKERS).split(",");
  }

  private Properties loadConfigs() {

    // TODO: Reload config with a FS watcher instead?, http://massapi.com/class/fi/FileChangedReloadingStrategy.html

    Properties properties = new Properties();

    for (String file : configFiles) {

      // first use the parent class loaded
      InputStream is = Preferences.class.getClassLoader().getResourceAsStream(file);

      // then check the classpath that was used to start the program
      if (is == null)
        is = ClassLoader.getSystemResourceAsStream(file);

      try {
        if (is == null) {
          // then check the absolute file path
          is = new FileInputStream(file);
        }
      } catch (FileNotFoundException ex) {}

      if (is != null) {

        Properties p2 = new Properties();

        try {
          p2.load(is);
          log.debug("Config file loaded: {}", file);
        } catch (IOException ex) {
          log.debug("Error handling config file", ex);
        } finally {
          try {
            is.close();
          } catch (Exception ex) {
            // TODO: instead of opening and closing this file constantly, check its timestamp first
          }
        }

        // merge values into one object
        properties.putAll(p2);

      } else {
        log.debug("Config file not loaded: {}", file);
      }
    }

    return properties;
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

  /**
   * Return a human readable list of config files
   * @return
   */
  public String getConfigs() {

    int k=configFiles.length;
    if (k==0)
      return null;
    StringBuilder out=new StringBuilder();
    out.append(configFiles[0]);
    for (int x=1;x<k;++x)
      out.append(", ").append(configFiles[x]);
    return out.toString();
  }

}
