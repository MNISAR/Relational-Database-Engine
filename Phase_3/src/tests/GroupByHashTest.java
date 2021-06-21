package tests;

import java.io.*;

import bufmgr.*;
import diskmgr.PCounter;
import global.*;
import hashindex.HIndex;
import hashindex.HashIndexWindowedScan;
import hashindex.HashKey;
import heap.*;
import index.IndexException;
import iterator.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

class GroupByHashDriver extends TestDriver
        implements GlobalConst {

    // groupBy_attr: 1
    // agg_list: 2
    // agg_types: MIN | MAX | AVG | SKYLINE

    private static float[][] data = {
            {1, 6, 8},
            {1, 4, 5},
            {2, 7, 8},
            {1, 4, 3},
            {3, 5, 10},
            {1, 2, 3},
            {2, 3, 4},
            {4, 8, 9},
            {3, 100, 20},
            {4, 5, 8}
    };

    private static int COLS;
    private static final String hFile = "hFile.in";
    private static AttrType[] attrType;
    private short[] attrSize;
    private static Heapfile  f = null;
    private static RID   rid;

    private static short REC_LEN1 = 32;
    private static short REC_LEN2 = 32;
    private static short REC_LEN3 = 32;

    private static FldSpec[] projlist;
    private static RelSpec rel = new RelSpec(RelSpec.outer);
    private static int _t_size;
    boolean status = false;

    TupleOrder[] order = new TupleOrder[2];
    AggType[] aggType = new AggType[4];

    public GroupByHashDriver() {
        super("groupByHashTest");
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);

        aggType[0] = new AggType(AggType.MIN);
        aggType[1] = new AggType(AggType.MAX);
        aggType[2] = new AggType(AggType.AVG);
        aggType[3] = new AggType(AggType.SKYLINE);
    }

    private void readDataIntoHeap(String fileName)
            throws IOException, InvalidTupleSizeException, InvalidTypeException, InvalidSlotNumberException, HFDiskMgrException, HFBufMgrException, HFException, HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, SpaceNotAvailableException {

        // Create the heap file object
        f = new Heapfile(hFile);
        File file = new File(fileName);
        Scanner sc = new Scanner(file);

        COLS = sc.nextInt();
        sc.nextLine(); // skipping the whole first line from the file as that has only 5 in it

        attrType = new AttrType[COLS];
        attrSize = new short[COLS];

        for(int i=0; i<attrType.length; i++){
            attrType[i] = new AttrType(AttrType.attrReal);
        }

        for(int i=0; i<attrSize.length; i++){
            attrSize[i] = 32;
        }

        projlist = new FldSpec[COLS];

        for(int i=0; i<attrType.length; i++){
            projlist[i] = new FldSpec(rel, i+1);
        }

        Tuple t = new Tuple();
        t.setHdr((short) COLS,attrType, attrSize);
        _t_size = t.size();

        t = new Tuple(_t_size);
        t.setHdr((short) COLS, attrType, attrSize);

        while (sc.hasNextLine()) {
            double[] doubleArray = Arrays.stream(Arrays.stream(sc.nextLine().trim()
                    .split("\\s+"))
                    .filter(s -> !s.isEmpty())
                    .toArray(String[]::new))
                    .mapToDouble(Double::parseDouble)
                    .toArray();

            for(int i=0; i<doubleArray.length; i++) {
                try {
                    t.setFloFld(i+1, (float) doubleArray[i]);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            rid = f.insertRecord(t.returnTupleByteArray());
        }
        System.out.println("Number of records in Database: "+f.getRecCnt());
        sc.close();
        SystemDefs.JavabaseBM.flushAllPages();
    }

    public boolean runTests () throws HFDiskMgrException, HFException, HFBufMgrException, IOException {

        System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");

        dbpath = "task3-grpByHash.minibase-db";

        SystemDefs sysdef = new SystemDefs( dbpath, 3000, 100, "Clock" );

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
            System.err.println (""+e);
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
            System.err.println (""+e);
        }

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
        }

        System.out.println ("\n" + "..." + testName() + " tests ");
        System.out.println (_pass==OK ? "completely successfully" : "failed");
        System.out.println (".\n\n");

        return _pass;
    }

    protected boolean test1()
    {
        System.out.println("------------------------ TEST 1 : MIN --------------------------");

        boolean status = OK;

        String heap_file_name = "test1GroupByHash.in";
        String index_file_name = "test1HashIndex.in";
        List<Tuple> result = new ArrayList<>();

        Heapfile hf = null;
        try {
            hf = new Heapfile(heap_file_name);

            attrType = new AttrType[3];
            attrType[0] = new AttrType (AttrType.attrReal);
            attrType[1] = new AttrType (AttrType.attrReal);
            attrType[2] = new AttrType (AttrType.attrReal);
            attrSize = null;
            Tuple t = new Tuple();
            t.setHdr((short) 3, attrType, null);

            for (int i=0; i<data.length; i++) {
                t.setFloFld(1, data[i][0]);
                t.setFloFld(2, data[i][1]);
                t.setFloFld(3, data[i][2]);

                hf.insertRecord(t.getTupleByteArray());
            }

            hf = null;

        } catch (IOException e) {
            e.printStackTrace();
        } catch (FieldNumberOutOfBoundException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (HFException e) {
            e.printStackTrace();
        } catch (InvalidSlotNumberException e) {
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        } catch (SpaceNotAvailableException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        } catch (InvalidTypeException e) {
            e.printStackTrace();
        }

        FldSpec groupByAttr = new FldSpec(new RelSpec(RelSpec.outer), 1);

        FldSpec[] aggList = new FldSpec[1];
        rel = new RelSpec(RelSpec.outer);
        aggList[0] = new FldSpec(rel, 2);

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[3];
        rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);

        // HashIndex Window Scan creation here
        try {
            HIndex h = new HIndex(index_file_name, AttrType.attrInteger, 10,10);
            Scan s = (new Heapfile(heap_file_name)).openScan();
            Tuple tup = new Tuple();
            rid = new RID();
            while((tup=s.getNext(rid))!=null){
                tup.setHdr((short)3, attrType, null);
                HashKey key = new HashKey(tup.getIntFld(1));
                h.insert(key, rid);
            }

            h.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        FldSpec[] out = {
                new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.outer), 2),
                new FldSpec(new RelSpec(RelSpec.outer), 3)
        };

        HashIndexWindowedScan hiwfs = null;
        try {
            hiwfs = new HashIndexWindowedScan(new IndexType(IndexType.Hash), heap_file_name, index_file_name, attrType, attrSize, attrType.length, out.length, out, null, 1, false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Sort operator working verified till here

        GroupByWithHash grpHash = null;
        PCounter.initialize();
        try {
            grpHash = new GroupByWithHash(attrType,
                    3,
                    null,
                    hiwfs,
                    groupByAttr,
                    aggList,
                    aggType[0],
                    projlist,
                    3,
                    5);


            try {
                result = grpHash.get_next_aggr();
            }
            catch (Exception e) {
                status = false;
                e.printStackTrace();
            }

            System.out.println("Group By Hash Results");

            while(result != null) {
                result.forEach((tuple) -> {
                    try {
                        tuple.print(attrType);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

                try {
                    result = grpHash.get_next_aggr();
                }
                catch (Exception e) {
                    status = false;
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                grpHash.close();
                hiwfs.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SortException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Number of Disk reads: "+ PCounter.get_rcounter());
        System.out.println("Number of Disk writes: "+ PCounter.get_wcounter());
        PCounter.initialize();

        System.err.println("------------------- TEST 1 completed ---------------------\n");

        return status;
    }


    protected boolean test2()
    {
        System.out.println("------------------------ TEST 2 : MAX --------------------------");
        boolean status = OK;

        String heap_file_name = "test2GroupByHash.in";
        String index_file_name = "test2HashIndex.in";
        List<Tuple> result = new ArrayList<>();

        Heapfile hf = null;
        try {
            hf = new Heapfile(heap_file_name);

            attrType = new AttrType[3];
            attrType[0] = new AttrType (AttrType.attrReal);
            attrType[1] = new AttrType (AttrType.attrReal);
            attrType[2] = new AttrType (AttrType.attrReal);
            attrSize = null;
            Tuple t = new Tuple();
            t.setHdr((short) 3, attrType, null);

            for (int i=0; i<data.length; i++) {
                t.setFloFld(1, data[i][0]);
                t.setFloFld(2, data[i][1]);
                t.setFloFld(3, data[i][2]);

                hf.insertRecord(t.getTupleByteArray());
            }

            hf = null;

        } catch (IOException e) {
            e.printStackTrace();
        } catch (FieldNumberOutOfBoundException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (HFException e) {
            e.printStackTrace();
        } catch (InvalidSlotNumberException e) {
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        } catch (SpaceNotAvailableException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        } catch (InvalidTypeException e) {
            e.printStackTrace();
        }

        FldSpec groupByAttr = new FldSpec(new RelSpec(RelSpec.outer), 1);

        FldSpec[] aggList = new FldSpec[1];
        rel = new RelSpec(RelSpec.outer);
        aggList[0] = new FldSpec(rel, 2);

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[3];
        rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);

        // HashIndex Window Scan creation here
        try {
            HIndex h = new HIndex(index_file_name, AttrType.attrInteger, 10,10);
            Scan s = (new Heapfile(heap_file_name)).openScan();
            Tuple tup = new Tuple();
            rid = new RID();
            while((tup=s.getNext(rid))!=null){
                tup.setHdr((short)3, attrType, null);
                HashKey key = new HashKey(tup.getIntFld(1));
                h.insert(key, rid);
            }

            h.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        FldSpec[] out = {
                new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.outer), 2),
                new FldSpec(new RelSpec(RelSpec.outer), 3)
        };

        HashIndexWindowedScan hiwfs = null;
        try {
            hiwfs = new HashIndexWindowedScan(new IndexType(IndexType.Hash), heap_file_name, index_file_name, attrType, attrSize, attrType.length, out.length, out, null, 1, false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Sort operator working verified till here

        GroupByWithHash grpHash = null;
        PCounter.initialize();
        try {
            grpHash = new GroupByWithHash(attrType,
                    3,
                    null,
                    hiwfs,
                    groupByAttr,
                    aggList,
                    aggType[1],
                    projlist,
                    3,
                    5);


            try {
                result = grpHash.get_next_aggr();
            }
            catch (Exception e) {
                status = false;
                e.printStackTrace();
            }

            System.out.println("Group By Hash Results");

            while(result != null) {
                result.forEach((tuple) -> {
                    try {
                        tuple.print(attrType);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

                try {
                    result = grpHash.get_next_aggr();
                }
                catch (Exception e) {
                    status = false;
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                grpHash.close();
                hiwfs.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SortException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Number of Disk reads: "+ PCounter.get_rcounter());
        System.out.println("Number of Disk writes: "+ PCounter.get_wcounter());
        PCounter.initialize();

        System.err.println("------------------- TEST 2 completed ---------------------\n");

        return status;
    }

    protected  boolean test3(){
        System.out.println("------------------------ TEST 3 : AVG --------------------------");
        boolean status = OK;

        String heap_file_name = "test3GroupByHash.in";
        String index_file_name = "test3HashIndex.in";
        List<Tuple> result = new ArrayList<>();

        Heapfile hf = null;
        try {
            hf = new Heapfile(heap_file_name);

            attrType = new AttrType[3];
            attrType[0] = new AttrType (AttrType.attrReal);
            attrType[1] = new AttrType (AttrType.attrReal);
            attrType[2] = new AttrType (AttrType.attrReal);
            attrSize = null;
            Tuple t = new Tuple();
            t.setHdr((short) 3, attrType, null);

            for (int i=0; i<data.length; i++) {
                t.setFloFld(1, data[i][0]);
                t.setFloFld(2, data[i][1]);
                t.setFloFld(3, data[i][2]);

                hf.insertRecord(t.getTupleByteArray());
            }

            hf = null;

        } catch (IOException e) {
            e.printStackTrace();
        } catch (FieldNumberOutOfBoundException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (HFException e) {
            e.printStackTrace();
        } catch (InvalidSlotNumberException e) {
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        } catch (SpaceNotAvailableException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        } catch (InvalidTypeException e) {
            e.printStackTrace();
        }

        FldSpec groupByAttr = new FldSpec(new RelSpec(RelSpec.outer), 1);

        FldSpec[] aggList = new FldSpec[1];
        rel = new RelSpec(RelSpec.outer);
        aggList[0] = new FldSpec(rel, 2);

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[3];
        rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);

        // HashIndex Window Scan creation here
        try {
            HIndex h = new HIndex(index_file_name, AttrType.attrInteger, 10,10);
            Scan s = (new Heapfile(heap_file_name)).openScan();
            Tuple tup = new Tuple();
            rid = new RID();
            while((tup=s.getNext(rid))!=null){
                tup.setHdr((short)3, attrType, null);
                HashKey key = new HashKey(tup.getIntFld(1));
                h.insert(key, rid);
            }

            h.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        FldSpec[] out = {
                new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.outer), 2),
                new FldSpec(new RelSpec(RelSpec.outer), 3)
        };

        HashIndexWindowedScan hiwfs = null;
        try {
            hiwfs = new HashIndexWindowedScan(new IndexType(IndexType.Hash), heap_file_name, index_file_name, attrType, attrSize, attrType.length, out.length, out, null, 1, false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Sort operator working verified till here

        GroupByWithHash grpHash = null;
        PCounter.initialize();
        try {
            grpHash = new GroupByWithHash(attrType,
                    3,
                    null,
                    hiwfs,
                    groupByAttr,
                    aggList,
                    aggType[2],
                    projlist,
                    3,
                    5);


            try {
                result = grpHash.get_next_aggr();
            }
            catch (Exception e) {
                status = false;
                e.printStackTrace();
            }

            System.out.println("Group By Hash Results");

            while(result != null) {
                result.forEach((tuple) -> {
                    try {
                        tuple.print(attrType);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

                try {
                    result = grpHash.get_next_aggr();
                }
                catch (Exception e) {
                    status = false;
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                grpHash.close();
                hiwfs.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SortException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Number of Disk reads: "+ PCounter.get_rcounter());
        System.out.println("Number of Disk writes: "+ PCounter.get_wcounter());
        PCounter.initialize();

        System.err.println("------------------- TEST 3 completed ---------------------\n");

        return status;
    }

    protected boolean test4(){
        System.out.println("------------------------ TEST 4 : SKYLINE --------------------------");
        boolean status = OK;

        String heap_file_name = "test4GroupByHash.in";
        String index_file_name = "test4HashIndex.in";
        List<Tuple> result = new ArrayList<>();

        Heapfile hf = null;
        try {
            hf = new Heapfile(heap_file_name);

            attrType = new AttrType[3];
            attrType[0] = new AttrType (AttrType.attrReal);
            attrType[1] = new AttrType (AttrType.attrReal);
            attrType[2] = new AttrType (AttrType.attrReal);
            attrSize = null;
            Tuple t = new Tuple();
            t.setHdr((short) 3, attrType, null);

            for (int i=0; i<data.length; i++) {
                t.setFloFld(1, data[i][0]);
                t.setFloFld(2, data[i][1]);
                t.setFloFld(3, data[i][2]);

                hf.insertRecord(t.getTupleByteArray());
            }

            hf = null;

        } catch (IOException e) {
            e.printStackTrace();
        } catch (FieldNumberOutOfBoundException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (HFException e) {
            e.printStackTrace();
        } catch (InvalidSlotNumberException e) {
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        } catch (SpaceNotAvailableException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        } catch (InvalidTypeException e) {
            e.printStackTrace();
        }

        FldSpec groupByAttr = new FldSpec(new RelSpec(RelSpec.outer), 1);

        FldSpec[] aggList = new FldSpec[1];
        rel = new RelSpec(RelSpec.outer);
        aggList[0] = new FldSpec(rel, 2);

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[3];
        rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);

        // HashIndex Window Scan creation here
        try {
            HIndex h = new HIndex(index_file_name, AttrType.attrInteger, 10,10);
            Scan s = (new Heapfile(heap_file_name)).openScan();
            Tuple tup = new Tuple();
            rid = new RID();
            while((tup=s.getNext(rid))!=null){
                tup.setHdr((short)3, attrType, null);
                HashKey key = new HashKey(tup.getIntFld(1));
                h.insert(key, rid);
            }

            h.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        FldSpec[] out = {
                new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.outer), 2),
                new FldSpec(new RelSpec(RelSpec.outer), 3)
        };

        HashIndexWindowedScan hiwfs = null;
        try {
            hiwfs = new HashIndexWindowedScan(new IndexType(IndexType.Hash), heap_file_name, index_file_name, attrType, attrSize, attrType.length, out.length, out, null, 1, false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Sort operator working verified till here

        GroupByWithHash grpHash = null;
        PCounter.initialize();
        try {
            grpHash = new GroupByWithHash(attrType,
                    3,
                    null,
                    hiwfs,
                    groupByAttr,
                    aggList,
                    aggType[3],
                    projlist,
                    3,
                    5);


            try {
                result = grpHash.get_next_aggr();
            }
            catch (Exception e) {
                status = false;
                e.printStackTrace();
            }

            System.out.println("Group By Hash Results");

            while(result != null) {
                result.forEach((tuple) -> {
                    try {
                        tuple.print(attrType);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

                try {
                    result = grpHash.get_next_aggr();
                }
                catch (Exception e) {
                    status = false;
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                grpHash.close();
                hiwfs.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SortException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Number of Disk reads: "+ PCounter.get_rcounter());
        System.out.println("Number of Disk writes: "+ PCounter.get_wcounter());
        PCounter.initialize();

        System.err.println("------------------- TEST 4 completed ---------------------\n");

        return status;
    }

    protected  boolean test5()
    {
        /*
        System.out.println("------------------------ TEST 5 --------------------------");
        try{
            readDataIntoHeap("../../data/data3.txt");
        }catch (Exception e){
            e.printStackTrace();
        }
        Tuple t = null;
        int num_attributes = COLS;
        int actual_pref_list[] = {1, 2};

        FldSpec[] projlist = new FldSpec[num_attributes+1];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for(int i=0;i<num_attributes;i++) projlist[i] = new FldSpec(rel, i+1);
        projlist[num_attributes] = new FldSpec(rel, 1);;

        AttrType[] attrType_for_proj = new AttrType[num_attributes];
        for(int i=0;i<num_attributes;i++) attrType_for_proj[i] = new AttrType(AttrType.attrReal);

        OurFileScan fscan = null;

        try {
            fscan = new OurFileScan(hFile, attrType_for_proj, null, (short) num_attributes, num_attributes, projlist, null, actual_pref_list);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        // Sort "test1sortPref.in"

        AttrType[] attrType_for_sort = new AttrType[num_attributes+1];
        for(int i=0;i<num_attributes;i++) {
            attrType_for_sort[i] = new AttrType(AttrType.attrReal);
        }
        attrType_for_sort[num_attributes] = new AttrType(AttrType.attrReal);

        Sort sort = null;
        try {
            sort = new Sort(attrType_for_sort, (short) (num_attributes+1), attrSize, fscan, (num_attributes+1), order[1], REC_LEN1, SORTPGNUM);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int count = 0;
        float[] outval = new float[5];

        try {
            t = sort.get_next();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        boolean flag = true;
        while (t != null) {
            try {
                outval[0] = t.getFloFld(1);
                outval[1] = t.getFloFld(2);
                outval[2] = t.getFloFld(3);
                outval[3] = t.getFloFld(4);
                outval[4] = 0;//t.getFloFld(3);

                System.out.println("Got row: "+outval[0]+" "+outval[1]+" "+outval[2]+" "+outval[3]+" "+outval[4]+" "+" | "+(outval[0]+outval[1]));
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            count++;

            try {
                t = sort.get_next();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

         */
        return true;
    }

    protected String testName()
    {
        return "GroupByHash";
    }
}

public class GroupByHashTest
{
    public static void main(String argv[]) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        boolean grpstatus;

        GroupByHashDriver hash = new GroupByHashDriver();

        grpstatus = hash.runTests();
        if (grpstatus != true) {
            System.out.println("Error ocurred during sorting tests");
        }
        else {
            System.out.println("Sorting tests completed successfully");
        }
    }
}

