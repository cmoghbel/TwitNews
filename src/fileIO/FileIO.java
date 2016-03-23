package fileIO;

import java.io.*;
import java.util.*;

/**
 * Reads local files into memory
 *
 * @author Lei (Ricky) Jin (rickyjin@cs.ucla.edu)
 */
public class FileIO {
  
  private String filePath = null;
  private File handle = null;
  
  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public File getHandle() {
    return handle;
  }

  public void setHandle(File handle) {
    this.handle = handle;
  }
  
  /**
   * Constructor for a new file instance
   * 
   * @param complete local file path as a string
   */
  public FileIO(String filePath) {
    this.filePath = filePath;
    this.handle = new File(filePath);
  }

  /**
  * Read words from file into a set to remove duplicates
  * 
  * @param can pass in empty or existing set
  */
  public Set<String> getWordSet(Set<String> set) {
    try {
      String line = null;
      BufferedReader inputFile =  new BufferedReader(new FileReader(handle));
      
      try {
        // Reads a line a time
        while (( line = inputFile.readLine()) != null){
          set.add(line.trim().toLowerCase());
        }
      }
      finally {
        inputFile.close();
      }
    }
    catch (IOException e){
      e.printStackTrace();
    }
    
    return set;
  }
} 