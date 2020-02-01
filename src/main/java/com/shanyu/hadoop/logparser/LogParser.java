package com.shanyu.hadoop.logparser;

import org.apache.commons.cli.*;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile;

/**
 * Parse Hadoop 2.x aggregated logs into txt files
 * 
 * Hadoop 2.x aggregated logs are stored as files in TFile format. All logs
 * belonging to an application is kept in one folder. We use TFileParser
 * to parse each file inside a folder into .txt format.
 * 
 * @author shzhao
 *
 */
public class LogParser 
{
  public final static String DEFAULT_REMOTE_FOLDER_PATH = "app-logs";
  public final static String DEFAULT_WASB_ACCOUNT_SUFFIX = "blob.core.windows.net";
  

  public static void main( String[] args )
      throws FileNotFoundException, IOException {
    Options options = new Options();
    options.addOption(new Option("a", "wasbAccount", true, "WASB Account Name"));
    options.addOption(new Option("k", "wasbKey", true, "WASB Account Key"));
    options.addOption(new Option("c", "wasbContainer", true, "WASB Container Name"));
    options.addOption(new Option("p", "wasbBlobPath", true,
      String.format("WASB Blob Path, defaults to '%s'", DEFAULT_REMOTE_FOLDER_PATH)));
    options.addOption(new Option("s", "wasbAccountSuffix", true,
      String.format("WASB Account suffix, defaults to '%s'", DEFAULT_WASB_ACCOUNT_SUFFIX)));
    options.addOption(new Option("i", "applicationId", true, "Application ID"));
    options.addOption(new Option("d", "localDir", true, "Local Directory Path"));
    Option output = new Option("o", "outputDir", true, "Output Directory Name (\"-\" prints to stdout)");
    options.addOption(output);
    
    CommandLine cmd = null;
    try {
      cmd = new PosixParser().parse(options, args);
    } catch(ParseException e) {
      System.out.println(e.getMessage());
      String usage = "hadooplogparser [-d <localDir>] [-a <wasbAccount> -k \"<wasbKey>\" -c <wasbContainer> -i <applicationId>] -o <outputDir>";
      new HelpFormatter().printHelp(usage, options);
      System.exit(1);
    }
    
    String outputDir = cmd.getOptionValue("outputDir");
    System.out.println("outputDir: " + outputDir);
    
    String localDir = cmd.getOptionValue("localDir");
    System.out.println("localDir: " + localDir);
    
    String wasbAccount = cmd.getOptionValue("wasbAccount");
    System.out.println("wasbAccount: " + wasbAccount);
    
    String wasbKey = cmd.getOptionValue("wasbKey");
    System.out.println("wasbKey: " + wasbKey);
    
    String wasbContainer = cmd.getOptionValue("wasbContainer");
    System.out.println("wasbContainer: " + wasbContainer);
    
    String applicationId = cmd.getOptionValue("applicationId");
    System.out.println("applicationId: " + applicationId);
    
    String wasbBlobPath =  cmd.getOptionValue("wasbBlobPath");
    if (isStrEmpty(wasbBlobPath)) {
      wasbBlobPath = DEFAULT_REMOTE_FOLDER_PATH;
    }
    System.out.println("wasbBlobPath: " + wasbBlobPath);
    
    String wasbAccountSuffix = cmd.getOptionValue("wasbAccountSuffix");
    if (isStrEmpty(wasbAccountSuffix)) {
      wasbAccountSuffix = DEFAULT_WASB_ACCOUNT_SUFFIX;
    }
    System.out.println("wasbAccountSuffix: " + wasbAccountSuffix);
    
    Configuration conf = new Configuration();
    String srcPathDir = null;
    if (!isStrEmpty(localDir)) {
      System.out.println("Parsing files from local folder: " + localDir);
      srcPathDir = localDir;
    }
    else if (!isStrEmpty(wasbAccount) && !isStrEmpty(wasbKey) && !isStrEmpty(wasbContainer) && !isStrEmpty(applicationId)) {
      String rootPathStr = String.format("wasb://%s@%s.%s/%s", wasbContainer, wasbAccount, wasbAccountSuffix, wasbBlobPath);
      conf.set(String.format("fs.azure.account.key.%s.%s", wasbAccount, wasbAccountSuffix), wasbKey);
      conf.set(String.format("fs.azure.account.keyprovider.%s.%s", wasbAccount, wasbAccountSuffix),
        "org.apache.hadoop.fs.azure.SimpleKeyProvider");
      Path rootPath = new Path(rootPathStr);
      FileSystem fs = rootPath.getFileSystem(conf);
      srcPathDir = findSubDir(fs, applicationId, rootPath);
      if (null == srcPathDir) {
        System.out.println(String.format("Error: could not find %s in %s", applicationId, rootPathStr));
        System.exit(2);
      }
    }
    else {
      System.out.println("You either need to provide localDir or (wasbAccount, wasbKey, wasbContainer, applicationId) as input.");
      System.exit(1);
    }
    
    Path path = new Path(srcPathDir);
    FileSystem fs = path.getFileSystem(conf);
    TFileParser parser = new TFileParser(conf, fs, outputDir);
    FileStatus[] files = fs.listStatus(path);
    for(FileStatus file : files) {
      try {
        parser.parseOneFile(file.getPath());
      }
      catch (IOException e) {
        System.out.println(String.format("Exception parsing file: %s, exception: %s",
          file.getPath().getName(), e.toString()));
      }
    }
  }
  
  // Find a sub directory named "name", under remote directory rootDir, recursively
  // Return the full path of the found directory.
  private static String findSubDir(FileSystem fs, String name, Path rootDir)
      throws FileNotFoundException, IOException {
    FileStatus[] files = fs.listStatus(rootDir);
    for (FileStatus s: files) {
      if (s.isDirectory()) {
        Path cur = s.getPath();
        if (cur.getName().equals(name)) {
          return cur.toString();
        }
        else {
          String found = findSubDir(fs, name, cur);
          if (null != found) {
            return found;
          }
        }
      }
    }
    return null;
  }
  
  private static boolean isStrEmpty(String str) {
    return (str == null || str.isEmpty());
  }
}
