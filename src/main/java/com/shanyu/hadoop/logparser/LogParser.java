package com.shanyu.hadoop.logparser;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileWriter;

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
  public static void main( String[] args ) throws Exception
  {
    if(args.length != 1) {
      System.out.println( "Parse hadoop logs and convert to text file" );
      System.out.println( "usage: logparser <app_folder>" );
      return;
    }
    
    String rootPathStr = args[0];
    Configuration conf = new Configuration();
    Path path = new Path(rootPathStr);
    FileSystem fs = path.getFileSystem(conf);
    FileStatus[] files = fs.listStatus(path);
    
    TFileParser parser = new TFileParser(conf, fs);
    for(FileStatus file : files) {
      if(file.getPath().getName().endsWith(".txt")) {
      //bypassing .txt files
        continue;
      }
      parser.parseOneFile(file.getPath());
    }
  }
}
