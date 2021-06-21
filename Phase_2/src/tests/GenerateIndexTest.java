package tests;

import java.io.*;
import java.util.*;
import java.lang.*;
import java.nio.charset.StandardCharsets;

import heap.*;
import global.*;
import btree.*;
import index.IndexScan;
import iterator.FldSpec;
import iterator.RelSpec;


/**
 * Note that in JAVA, methods can't be overridden to be more private.
 * Therefore, the declaration of all private functions are now declared
 * protected as opposed to the private type in C++.
 */

//watching point: RID rid, some of them may not have to be newed.

class GenerateIndexDriver implements GlobalConst {

    public BTreeFile file;
    public int keyType;

    protected String dbpath;
    protected String logpath;

    public void runTests() {
        Random random = new Random();
        dbpath = "BTREE" + random.nextInt() + ".minibase-db";
        logpath = "BTREE" + random.nextInt() + ".minibase-log";


        SystemDefs sysdef = new SystemDefs(dbpath, 200, NUMBUF, "Clock");
        System.out.println("\n" + "Running " + " tests...." + "\n");

        keyType = AttrType.attrInteger;

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
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        //This step seems redundant for me.  But it's in the original
        //C++ code.  So I am keeping it as of now, just in case I
        //I missed something
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        //Run the tests. Return type different from C++
        runAllTests();

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        System.out.print("\n" + "..." + " Finished ");
        System.out.println(".\n\n");


    }

    protected void runAllTests() {
        try{
//            test1();
//            test2();
            test3();
//            test4();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    // creating combined BTreeIndex
    void test1()
            throws Exception {
        try {
            System.out.println(" Combined BTreeIndex creation");
            int [] pref_list = new int[] {1,1,1};
            int pref_list_length = 3;

            GenerateIndexFiles obj = new GenerateIndexFiles();
            IndexFile hf = obj.createCombinedBTreeIndex("/Users/kunjpatel/Desktop/CSE510_DBMSi/Phase_2/data/subset2.txt", pref_list, pref_list_length);

        } catch (Exception e) {
            throw e;
        }


    }

    // creating individual BTreeIndex
    void test2()
            throws Exception {
        try {
            System.out.println(" BTreeIndex creation");

            GenerateIndexFiles obj = new GenerateIndexFiles();
            IndexFile[] hf = obj.createBTreeIndex("/Users/kunjpatel/Desktop/CSE510_DBMSi/Phase_2/data/subset2.txt");

        } catch (Exception e) {
            throw e;
        }
    }

    // scanning in combined BTree
    void test3()
            throws Exception {
        try {
            BTFileScan scan;
            KeyDataEntry entry;
            System.out.println("CombinedBTreeIndex scanning");
            int [] pref_list = new int[] {1,1,1};
            int pref_list_length = 3;

            GenerateIndexFiles obj = new GenerateIndexFiles();
            IndexFile indexFile = obj.createCombinedBTreeIndex("/Users/kunjpatel/Desktop/CSE510_DBMSi/Phase_2/data/subset2.txt",pref_list, pref_list_length);
            System.out.println("Index created! ");
            scan = ((BTreeFile) indexFile).new_scan(null, null);
             
            
            Heapfile hf = new Heapfile("heap_" + "AAA");
            Scan heap_scan = new Scan(hf);
            
            RID rid;
            entry = scan.get_next();
            
            Tuple t = new Tuple();
            short [] Ssizes = null;
            
            AttrType [] Stypes = new AttrType[pref_list_length];
            for(int i=0;i<pref_list_length;i++){Stypes[i] = new AttrType (AttrType.attrReal);}
            
            t.setHdr((short)pref_list_length, Stypes, Ssizes);            
            int size = t.size();
            
            t = new Tuple(size);
            t.setHdr((short)pref_list_length, Stypes, Ssizes);  
            
            while (entry != null) {
            	rid = ((LeafData) entry.data).getData();
            	
            	t.tupleCopy(hf.getRecord(rid));
            	t.print(Stypes); 
            	
                System.out.println("SCAN RESULT: " + entry.key + " > " + entry.data);
                entry = scan.get_next();

            }
            System.out.println("AT THE END OF SCAN!");
            
            

//      Have to add code to SCAN the heap file using the BTree Index

            /*
            TODO: To be fixed
            FldSpec[] projlist = new FldSpec[3];
            RelSpec rel = new RelSpec(RelSpec.outer);
            projlist[0] = new FldSpec(rel, 1);
            projlist[1] = new FldSpec(rel, 2);
            projlist[1] = new FldSpec(rel, 3);
            int COLS = 3;
            AttrType[] Stypes = new AttrType[COLS];
            for (int i = 0; i < COLS; i++) {
                Stypes[i] = new AttrType(AttrType.attrReal);
            }

            IndexScan iscan = new IndexScan(new IndexType(IndexType.B_Index), "AAA" + (obj.prefix - 1), "BTreeIndex", Stypes, null, 3, 2, projlist, null, 3, true);
            int count = 0;
            Tuple t = null;
            String outval = null;
            t = iscan.get_next();
            boolean flag = true;

            while (t != null) {
                t = iscan.get_next();
                System.out.println(t.noOfFlds());
            }
             */

        } catch (Exception e){
            e.printStackTrace();
        }
    }

    // scanning in BTree on each index
    void test4()
            throws Exception {
        try {
            KeyDataEntry entry;
            System.out.println("BTreeIndex scanning");

            GenerateIndexFiles obj = new GenerateIndexFiles();
            IndexFile[] hf = obj.createBTreeIndex("/Users/kunjpatel/Desktop/CSE510_DBMSi/Phase_2/data/subset2.txt");
            BTFileScan[] scans = new BTFileScan[hf.length];
            for(int i=0;i<hf.length;i++){
                scans[i] = ((BTreeFile)hf[i]).new_scan(null, null);
            }
            for(BTFileScan scan: scans) {
                entry = scan.get_next();
                while (entry != null) {
                    System.out.println("SCAN RESULT: " + entry.key + " " + entry.data);
                    entry = scan.get_next();
                }
                System.out.println("AT THE END OF SCAN!");
            }
        } catch (Exception e) {
            throw e;
        }
    }

}


/**
 * To get the integer off the command line
 */

public class GenerateIndexTest implements GlobalConst {

    public static void main(String[] argvs) {

        try {
            GenerateIndexDriver btdriver = new GenerateIndexDriver();
            btdriver.runTests();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error encountered during buffer manager tests:\n");
            Runtime.getRuntime().exit(1);
        }
    }

}