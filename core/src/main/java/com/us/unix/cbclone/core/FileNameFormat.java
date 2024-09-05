package com.us.unix.cbclone.core;

import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FileNameFormat {
  public static String getFileAbsPath(String filename, boolean overwrite) {
    filename = FilenameUtils.normalize(filename);
    File file = new File(filename);

    if (!FilenameUtils.isExtension(filename, "gz")) {
      filename = filename + ".gz";
      file = new File(filename);
    }

    if (!file.isAbsolute()) {
      String homeDirectory = System.getProperty("user.home");
      String pathPrefix = FilenameUtils.concat(homeDirectory, "cbclone");
      filename = FilenameUtils.concat(pathPrefix, filename);
      file = new File(filename);
    }

    try {
      Files.createDirectories(Paths.get(file.getParent()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (file.exists() && !overwrite) {
      String fileBase = FilenameUtils.getBaseName(filename);
      String fileExt = FilenameUtils.getExtension(filename);
      String dateFileNameFormat = "yyyyMMddHHmmss";
      SimpleDateFormat fileNameFormat = new SimpleDateFormat(dateFileNameFormat);
      Date date = new Date();
      String timeStamp = fileNameFormat.format(date);
      String newFileBase = String.format("%s-%s.%s", fileBase, timeStamp, fileExt);
      filename = FilenameUtils.concat(file.getParent(), newFileBase);
      file = new File(filename);
    }

    return file.getAbsolutePath();
  }

  public static String readFileAbsPath(String filename) {
    filename = FilenameUtils.normalize(filename);
    File file = new File(filename);

    if (!FilenameUtils.isExtension(filename, "gz")) {
      filename = filename + ".gz";
      file = new File(filename);
    }

    if (!file.isAbsolute()) {
      String homeDirectory = System.getProperty("user.home");
      String pathPrefix = FilenameUtils.concat(homeDirectory, "cbclone");
      filename = FilenameUtils.concat(pathPrefix, filename);
      file = new File(filename);
    }

    if (!file.exists()) {
     throw new RuntimeException(String.format("File %s does not exist", file.getAbsolutePath()));
    }

    return file.getAbsolutePath();
  }
}
