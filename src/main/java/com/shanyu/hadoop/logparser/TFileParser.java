package com.shanyu.hadoop.logparser;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile;

/**
 * Parse TFile's content to key value pair
 * @author shzhao
 *
 */
public class TFileParser {
  FileSystem _fs;
  Configuration _conf;
  
  public TFileParser(Configuration conf, FileSystem fs) {
    _conf = conf;
    _fs = fs;
  }
  
  void parseOneFile(Path path) 
      throws Exception {
    String outFileName = path.toString();
    System.out.println("converting " + outFileName);
    if(outFileName.startsWith("file:/")) {
      //get rid of file:/
      outFileName = outFileName.substring(6);
    }
    outFileName += ".txt";
    System.out.println("writting to " + outFileName);
    
    try (
      final BufferedWriter outWriter= new BufferedWriter(new FileWriter(outFileName));
      final FSDataInputStream fsdis = _fs.open(path);
      final TFile.Reader reader =
        new TFile.Reader(fsdis, _fs.getFileStatus(path).getLen(), _conf);
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
