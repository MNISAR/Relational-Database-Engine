package tests;

import java.io.*;

import bufmgr.*;
import diskmgr.*;
import global.*;
import heap.*;
import index.*;
import iterator.*;
import java.util.*;
import btree.*;


 class TopKNraJoinDriver extends TestDriver
implements GlobalConst {
	 
	public TopKNraJoinDriver() {
	     super("TopKNraJoinDriver");
	}
	
	public boolean runTests () throws HFDiskMgrException, HFException, HFBufMgrException, IOException {

        System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");
        SystemDefs sysdef = new SystemDefs( dbpath, 8000, 100, "Clock" );

        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        boolean _pass = runAllTests();

        System.out.println ("\n" + "..." + testName() + " tests ");
        System.out.println (_pass==OK ? "completely successfully" : "failed");
        System.out.println (".\n\n");

        return _pass;
    }
	
	protected boolean test1() {
        System.out.println("------------------------ TEST 2 --------------------------");
        
        BTFileScan scan1;
        BTFileScan scan2;

        KeyDataEntry entry1;
        KeyDataEntry entry2;
        System.out.println("CombinedBTreeIndex scanning");
        
//        int [] pref_list1 = new int[] {0,1};
//        int pref_list_length1 = 2;
//        
//        int [] pref_list2 = new int[] {0,1};
//        int pref_list_length2 = 2;

//        GenerateIndexFiles obj = new GenerateIndexFiles();
//        IndexFile indexFile1 = null;
//        IndexFile indexFile2 = null;
//        
//		try {
//			indexFile1 = obj.createCombinedBTreeIndex("/Users/kunjpatel/Desktop/CSE510_DBMSi/Phase_3/data/subset2.txt",pref_list1, pref_list_length1);
//			indexFile2 = obj.createCombinedBTreeIndex("/Users/kunjpatel/Desktop/CSE510_DBMSi/Phase_3/data/temp.txt",pref_list2, pref_list_length2);
//		} catch (Exception e) {
//			e.printStackTrace();
//		} 
//		
//        System.out.println("Index created! ");
//        
//        try {
//        	scan1 = ((BTreeFile) indexFile1).new_scan(null, null);
//        	scan2 = ((BTreeFile) indexFile2).new_scan(null, null);
//		} catch (Exception e) {
//			e.printStackTrace();
//		} 
        
        AttrType[] attrType = new AttrType[2];
        attrType[0] = new AttrType(AttrType.attrReal);
        attrType[1] = new AttrType(AttrType.attrReal);
        
        short[] attrSize = new short[2];
        
        FldSpec joinAttr = new FldSpec(new RelSpec(RelSpec.outer), 1);
        FldSpec mergeAttr = new FldSpec(new RelSpec(RelSpec.outer), 2);
        
        /**
         * AttrType[] in1, int len_in1, short[] t1_str_sizes,
			FldSpec joinAttr1,
			FldSpec mergeAttr1,
			AttrType[] in2, int len_in2, short[] t2_str_sizes,
			FldSpec joinAttr2,
			FldSpec mergeAttr2,
			java.lang.String relationName1,
			java.lang.String relationName2,
			int k,
			int n_pages
         */
        TopK_NRAJoin tknj = new TopK_NRAJoin(
							        attrType, 2,attrSize,
							        joinAttr,
							        mergeAttr,
							        attrType, 2,attrSize,
							        joinAttr,
							        mergeAttr,
							        "heap_AAA1",
							        "heap_AAA2",2, 10
        );
        
        try {
			tknj.calculateTopKJoins();
		} catch (Exception e) {
			e.printStackTrace();
		} 
        
        boolean status = OK;
		return status;
    }
	
	protected boolean test2() {
        System.out.println("------------------------ TEST 2 --------------------------");

        boolean status = OK;
		return status;
    }
	
	protected boolean test3() {
        System.out.println("------------------------ TEST 2 --------------------------");

        boolean status = OK;
		return status;
    }
	
	
}


public class TopKNraJoinTest  {
	
	public static void main(String[] argv) throws HFDiskMgrException, HFException, HFBufMgrException, IOException {
		TopKNraJoinDriver tknj = new TopKNraJoinDriver();
		tknj.runTests();
	}

}