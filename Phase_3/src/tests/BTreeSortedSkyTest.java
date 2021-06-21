package tests;

import java.io.*;
import java.util.*;
import java.lang.*;
import heap.*;
import iterator.Iterator;
import bufmgr.*;
import diskmgr.*;
import global.*;
import btree.*;

class BTSortedSkyDriver extends TestDriver
implements GlobalConst
{

  public BTreeFile file;
  public int postfix=0;
  public int keyType;
  public BTFileScan scan;
  
  protected String dbpath;  
  protected String logpath;
  public int deleteFashion;
  
  public boolean runTests () {
    Random random = new Random();
    dbpath = "BTREE" + random.nextInt() + ".minibase-db"; 
    logpath = "BTREE" + random.nextInt() + ".minibase-log"; 
    
    SystemDefs sysdef = new SystemDefs( dbpath, 5000 ,5000,"Clock");  
    System.out.println ("\n" + "Running " + " tests...." + "\n");
    
    keyType=AttrType.attrInteger;
    
    // Kill anything that might be hanging around
    String newdbpath;
    String newlogpath;
    String remove_logcmd;
    String remove_dbcmd;
    String remove_cmd = "/bin/rm -rf ";
    
    newdbpath = dbpath;
    newlogpath = logpath;

    remove_logcmd = remove_cmd + logpath;
    remove_dbcmd = remove_cmd + dbpath;
    
    // Commands here is very machine dependent.  We assume
    // user are on UNIX system here
    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    }
    catch (IOException e) {
      System.err.println ("IO error: "+e);
    }
    
    remove_logcmd = remove_cmd + newlogpath;
    remove_dbcmd = remove_cmd + newdbpath;
    
    //This step seems redundant for me.  But it's in the original
    //C++ code.  So I am keeping it as of now, just in case I
    //I missed something
    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    }
    catch (IOException e) {
      System.err.println ("IO error: "+e);
    }
    
    //Run the tests. Return type different from C++
    runAllTests();
    
    //Clean up again
    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    }
    catch (IOException e) {
      System.err.println ("IO error: "+e);
    }
    
    System.out.print ("\n" + "..." + " Finished ");
    System.out.println (".\n\n");
    
    return true;
    
  }

  
 protected boolean runAllTests () {
	  
	  boolean res1 = test1();
	  boolean res2 = test2();
	  boolean res3 = test3();
	  boolean res4 = test4();
	  
	  return res1 && res2 && res3 && res4;
}

protected boolean test1 () {
	BTFileScan scan;
    KeyDataEntry entry;
    System.out.println("CombinedBTreeIndex scanning");
    int [] pref_list = new int[] {1,1,1,0,0};
    int pref_list_length = 5;
    
    try {
  	  
	      GenerateIndexFiles obj = new GenerateIndexFiles();
	      IndexFile indexFile = obj.createCombinedBTreeIndex("../../data/data1.txt",pref_list, pref_list_length);
	      System.out.println("Index created! ");
	      scan = ((BTreeFile) indexFile).new_scan(null, null);
	      
	      Heapfile hf = new Heapfile("heap_" + "AAA");
	      
	      RID rid;
	      entry = scan.get_next();
	      
	      Tuple t = new Tuple();
	      short [] Ssizes = null;
	      
	      AttrType [] attrType = new AttrType[pref_list_length];
	      for(int i=0;i<pref_list_length;i++){attrType[i] = new AttrType (AttrType.attrReal);}
	      
	      t.setHdr((short)pref_list_length, attrType, Ssizes);            
	      int size = t.size();
	      
	      t = new Tuple(size);
	      t.setHdr((short)pref_list_length, attrType, Ssizes);  
	      
	      BTreeSortedSky btree = new BTreeSortedSky(attrType, pref_list_length, Ssizes, 0, null, "heap_AAA", pref_list, pref_list_length, indexFile, 2 );
	      
	      btree.computeSkylines();
	  
	      System.out.println("AT THE END OF SCAN!");
    
    } catch (Exception e) {
  	  e.printStackTrace();
    }
    
    return true;

}

protected boolean test2 () {
	BTFileScan scan;
    KeyDataEntry entry;
    System.out.println("CombinedBTreeIndex scanning");
    int [] pref_list = new int[] {1,0};
    int pref_list_length = 2;
    
    try {
  	  
	      GenerateIndexFiles obj = new GenerateIndexFiles();
	      IndexFile indexFile = obj.createCombinedBTreeIndex("../../data/data_large_skyline.txt",pref_list, pref_list_length);
	      System.out.println("Index created! ");
	      scan = ((BTreeFile) indexFile).new_scan(null, null);
	      
	      Heapfile hf = new Heapfile("heap_" + "AAA");
	      
	      RID rid;
	      entry = scan.get_next();
	      
	      Tuple t = new Tuple();
	      short [] Ssizes = null;
	      
	      AttrType [] attrType = new AttrType[pref_list_length];
	      for(int i=0;i<pref_list_length;i++){attrType[i] = new AttrType (AttrType.attrReal);}
	      
	      t.setHdr((short)pref_list_length, attrType, Ssizes);            
	      int size = t.size();
	      
	      t = new Tuple(size);
	      t.setHdr((short)pref_list_length, attrType, Ssizes);  
	      
	      BTreeSortedSky btree = new BTreeSortedSky(attrType, pref_list_length, Ssizes, 0, null, "heap_AAA", pref_list, pref_list_length, indexFile, 2 );
	      
	      btree.computeSkylines();
	  
	      System.out.println("AT THE END OF SCAN!");
    
    } catch (Exception e) {
  	  e.printStackTrace();
    }
    
    return true;
}

protected boolean test3 () {
	BTFileScan scan;
    KeyDataEntry entry;
    System.out.println("CombinedBTreeIndex scanning");
    int [] pref_list = new int[] {1,0,1,0,1};
    int pref_list_length = 5;
    
    try {
  	  
	      GenerateIndexFiles obj = new GenerateIndexFiles();
	      IndexFile indexFile = obj.createCombinedBTreeIndex("../../data/data3.txt",pref_list, pref_list_length);
	      System.out.println("Index created! ");
	      scan = ((BTreeFile) indexFile).new_scan(null, null);
	      
	      Heapfile hf = new Heapfile("heap_" + "AAA");
	      
	      RID rid;
	      entry = scan.get_next();
	      
	      Tuple t = new Tuple();
	      short [] Ssizes = null;
	      
	      AttrType [] attrType = new AttrType[pref_list_length];
	      for(int i=0;i<pref_list_length;i++){attrType[i] = new AttrType (AttrType.attrReal);}
	      
	      t.setHdr((short)pref_list_length, attrType, Ssizes);            
	      int size = t.size();
	      
	      t = new Tuple(size);
	      t.setHdr((short)pref_list_length, attrType, Ssizes);  
	      
	      BTreeSortedSky btree = new BTreeSortedSky(attrType, pref_list_length, Ssizes, 0, null, "heap_AAA", pref_list, pref_list_length, indexFile, 2 );
	      
	      btree.computeSkylines();
	  
	      System.out.println("AT THE END OF SCAN!");
    
    } catch (Exception e) {
  	  e.printStackTrace();
    }
    
    return true;
}

protected boolean test4 () {
	BTFileScan scan;
    KeyDataEntry entry;
    System.out.println("CombinedBTreeIndex scanning");
    int [] pref_list = new int[] {1,0,1,0,1};
    int pref_list_length = 5;
    
    try {
  	  
	      GenerateIndexFiles obj = new GenerateIndexFiles();
	      IndexFile indexFile = obj.createCombinedBTreeIndex("../../data/data2.txt",pref_list, pref_list_length);
	      System.out.println("Index created! ");
	      scan = ((BTreeFile) indexFile).new_scan(null, null);
	      
	      Heapfile hf = new Heapfile("heap_" + "AAA");
	      
	      RID rid;
	      entry = scan.get_next();
	      
	      Tuple t = new Tuple();
	      short [] Ssizes = null;
	      
	      AttrType [] attrType = new AttrType[pref_list_length];
	      for(int i=0;i<pref_list_length;i++){attrType[i] = new AttrType (AttrType.attrReal);}
	      
	      t.setHdr((short)pref_list_length, attrType, Ssizes);            
	      int size = t.size();
	      
	      t = new Tuple(size);
	      t.setHdr((short)pref_list_length, attrType, Ssizes);  
	      
	      BTreeSortedSky btree = new BTreeSortedSky(attrType, pref_list_length, Ssizes, 0, null, "heap_AAA", pref_list, pref_list_length, indexFile, 2 );
	      
	      btree.computeSkylines();
	  
	      System.out.println("AT THE END OF SCAN!");
    
    } catch (Exception e) {
  	  e.printStackTrace();
  	  return true;
    }
    
    return true;
	}
}

public class BTreeSortedSkyTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try{ 
			BTSortedSkyDriver bttest = new BTSortedSkyDriver();
		      bttest.runTests();	
		    }
		    catch (Exception e) {
		      e.printStackTrace();
		      System.err.println ("Error encountered during buffer manager tests:\n");
		      Runtime.getRuntime().exit(1);
		    }

	}

}
