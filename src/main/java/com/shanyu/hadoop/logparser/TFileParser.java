package com.shanyu.hadoop.logparser;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile;

/**
 * Parse TFile's content to key value pair, and store in local destLocalFolder
 */
public class TFileParser {
  private FileSystem _fs;
  private Configuration _conf;
  private String _destLocalFolder;
  
  public TFileParser(Configuration conf, FileSystem fs, String destLocalFolder) {
    _conf = conf;
    _fs = fs;
    _destLocalFolder = destLocalFolder;
  }
  
  void parseOneFile(Path srcPath)
      throws FileNotFoundException, IOException {
    String destLocalPath = _destLocalFolder + File.separator + srcPath.getName() + ".txt";
    System.out.println("===================================");
    System.out.println("converting " + srcPath);
    System.out.println("writting to " + destLocalPath);
    
    try (
      final FSDataInputStream fsdis = _fs.open(srcPath);
      final OutputStreamWriter outWriter = new OutputStreamWriter(new FileOutputStream(destLocalPath), "utf-8");
      final TFile.Reader reader =
        new TFile.Reader(fsdis, _fs.getFileStatus(srcPath).getLen(), _conf);
      final TFile.Reader.Scanner scanner = reader.createScanner()
    ) {
      int count = 0;
      while(!scanner.atEnd()) {
        if(count != 3) {
          //skip VERSION, APPLICATION_ACL, and APPLICATION_OWNER
          scanner.advance();
          count++;
          continue;
        }
        
        TFile.Reader.Scanner.Entry entry = scanner.entry();
        
        try (
            final DataInputStream disKey = entry.getKeyStream();
            final DataInputStream disValue = entry.getValueStream();
        ) {
          outWriter.write("Container: ");
          outWriter.write(disKey.readUTF());
          outWriter.write("\r\n");
          
          while(disValue.available() > 0) {
            String strFileName = disValue.readUTF();
            String strLength = disValue.readUTF();
            outWriter.write("=====================================================");
            outWriter.write("\r\n");
            outWriter.write("File Name: " + strFileName);
            outWriter.write("\r\n");
            outWriter.write("File Length: " + strLength);
            outWriter.write("\r\n");
            outWriter.write("-----------------------------------------------------");
            outWriter.write("\r\n");
  
            byte[] buf = new byte[65535];
            int lenRemaining = Integer.parseInt(strLength);
            while(lenRemaining > 0) {
              int len = disValue.read(buf, 0, lenRemaining>65535?65535:lenRemaining);
              if(len > 0) {
                outWriter.write(new String(buf, 0, len, "UTF-8"));
                lenRemaining -= len;
              }
              else {
                break;
              }
            }
            
            outWriter.write("\r\n");
          }
        }
        break;
      } //end of while(!scanner.atEnd())
    }
  }
}
