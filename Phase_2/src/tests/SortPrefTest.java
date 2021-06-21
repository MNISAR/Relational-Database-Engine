package tests;

import java.io.*;

import bufmgr.*;
import driver.AppDriver;
import global.*;
import heap.*;
import iterator.*;
import driver.*;

import java.util.Arrays;
import java.util.Scanner;


class SORTPrefDriver extends TestDriver
        implements GlobalConst {

    // [0,1]

    private static float[][] data1 = {
            {0.825f, 0.823f, 0.453f, 0.122f, 0.356f},   // 1.648
            {0.855f, 0.316f, 0.782f, 0.478f, 0.758f},   // 1.171
            {0.011f, 0.268f, 0.348f, 0.646f, 0.161f},   // 0.279
            {0.896f, 0.572f, 0.281f, 0.592f, 0.166f},   // 1.468
            {0.852f, 0.194f, 0.613f, 0.846f, 0.846f},   // 1.046
            {0.110f, 0.758f, 0.221f, 0.234f, 0.169f},   // 0.868
            {0.259f, 0.550f, 0.837f, 0.138f, 0.960f},   // 0.809
            {0.821f, 0.814f, 0.104f, 0.106f, 0.475f},   // 1.635
            {0.192f, 0.975f, 0.761f, 0.157f, 0.899f},   // 1.167
            {0.627f, 0.043f, 0.133f, 0.690f, 0.272f}    // 0.67
    };

    private static float[][] data2 = {
            {0.011f, 0.268f, 0.348f, 0.646f, 0.161f},   // 0.279
            {0.627f, 0.043f, 0.133f, 0.690f, 0.272f},   // 0.67
            {0.259f, 0.550f, 0.837f, 0.138f, 0.960f},   // 0.809
            {0.110f, 0.758f, 0.221f, 0.234f, 0.169f},   // 0.868
            {0.852f, 0.194f, 0.613f, 0.846f, 0.846f},   // 1.046
            {0.192f, 0.975f, 0.761f, 0.157f, 0.899f},   // 1.167
            {0.855f, 0.316f, 0.782f, 0.478f, 0.758f},   // 1.171
            {0.896f, 0.572f, 0.281f, 0.592f, 0.166f},   // 1.468
            {0.821f, 0.814f, 0.104f, 0.106f, 0.475f},   // 1.635
            {0.825f, 0.823f, 0.453f, 0.122f, 0.356f},   // 1.648
    };

    private static float[][] data3 = {
            {0.825f, 0.823f, 0.453f, 0.122f, 0.356f},   // 1.648
            {0.821f, 0.814f, 0.104f, 0.106f, 0.475f},   // 1.635
            {0.896f, 0.572f, 0.281f, 0.592f, 0.166f},   // 1.468
            {0.855f, 0.316f, 0.782f, 0.478f, 0.758f},   // 1.171
            {0.192f, 0.975f, 0.761f, 0.157f, 0.899f},   // 1.167
            {0.852f, 0.194f, 0.613f, 0.846f, 0.846f},   // 1.046
            {0.110f, 0.758f, 0.221f, 0.234f, 0.169f},   // 0.868
            {0.259f, 0.550f, 0.837f, 0.138f, 0.960f},   // 0.809
            {0.627f, 0.043f, 0.133f, 0.690f, 0.272f},   // 0.67
            {0.011f, 0.268f, 0.348f, 0.646f, 0.161f}    // 0.279
    };

    // [0,1,2]

    private static float[][] data4 = {
            {0.825f, 0.823f, 0.453f, 0.122f, 0.356f, 2.101f},
            {0.855f, 0.316f, 0.782f, 0.478f, 0.758f, 1.953f},
            {0.011f, 0.268f, 0.348f, 0.646f, 0.161f, 0.627f},
            {0.896f, 0.572f, 0.281f, 0.592f, 0.166f, 1.749f},
            {0.852f, 0.194f, 0.613f, 0.846f, 0.846f, 1.659f},
            {0.110f, 0.758f, 0.221f, 0.234f, 0.169f, 1.089f},
            {0.259f, 0.550f, 0.837f, 0.138f, 0.960f, 1.646f},
            {0.821f, 0.814f, 0.104f, 0.106f, 0.475f, 1.739f},
            {0.192f, 0.975f, 0.761f, 0.157f, 0.899f, 1.928f},
            {0.627f, 0.043f, 0.133f, 0.690f, 0.272f, 0.803f}
    };

    private static float[][] data5 = {
            {0.011f, 0.268f, 0.348f, 0.646f, 0.161f},   // 0.627
            {0.627f, 0.043f, 0.133f, 0.690f, 0.272f},   // 0.803
            {0.110f, 0.758f, 0.221f, 0.234f, 0.169f},   // 1.089
            {0.259f, 0.550f, 0.837f, 0.138f, 0.960f},   // 1.646
            {0.852f, 0.194f, 0.613f, 0.846f, 0.846f},   // 1.659
            {0.821f, 0.814f, 0.104f, 0.106f, 0.475f},   // 1.739
            {0.896f, 0.572f, 0.281f, 0.592f, 0.166f},   // 1.749
            {0.192f, 0.975f, 0.761f, 0.157f, 0.899f},   // 1.928
            {0.855f, 0.316f, 0.782f, 0.478f, 0.758f},   // 1.953
            {0.825f, 0.823f, 0.453f, 0.122f, 0.356f},   // 2.101
    };

    private static float[][] data6 = {
            {0.825f, 0.823f, 0.453f, 0.122f, 0.356f},   // 2.101
            {0.855f, 0.316f, 0.782f, 0.478f, 0.758f},   // 1.953
            {0.192f, 0.975f, 0.761f, 0.157f, 0.899f},   // 1.928
            {0.896f, 0.572f, 0.281f, 0.592f, 0.166f},   // 1.749
            {0.821f, 0.814f, 0.104f, 0.106f, 0.475f},   // 1.739
            {0.852f, 0.194f, 0.613f, 0.846f, 0.846f},   // 1.659
            {0.259f, 0.550f, 0.837f, 0.138f, 0.960f},   // 1.646
            {0.110f, 0.758f, 0.221f, 0.234f, 0.169f},   // 1.089
            {0.627f, 0.043f, 0.133f, 0.690f, 0.272f},   // 0.803
            {0.011f, 0.268f, 0.348f, 0.646f, 0.161f}    // 0.627
    };


    private static int COLS;
    private static final String hFile = "hFile.in";
    private static AttrType[] attrType;
    private short[] attrSize;
    private static Heapfile  f = null;
    private static RID   rid;

    private static int   NUM_RECORDS = data2.length;
    private static short REC_LEN1 = 32;
    private static short REC_LEN2 = 32;
    private static short REC_LEN3 = 32;
    private static short REC_LEN4 = 32;
    private static short REC_LEN5 = 32;
    private static int   SORTPGNUM = 20;
    private static FldSpec[] projlist;
    private static RelSpec rel = new RelSpec(RelSpec.outer);
    private static int _t_size;
    boolean status = false;

    TupleOrder[] order = new TupleOrder[2];

    public SORTPrefDriver() {
        super("sorttest");
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);
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
            projlist[i] = new FldSpec(rel, i+1);;
        }

        Tuple t = new Tuple();
        t.setHdr((short) COLS,attrType, attrSize);
        int size = t.size();
        _t_size = t.size();

        t = new Tuple(size);
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
        System.out.println("------------------------ TEST 1 --------------------------");

        boolean status = OK;

        AttrType[] attrType = new AttrType[5];
        attrType[0] = new AttrType(AttrType.attrReal);
        attrType[1] = new AttrType(AttrType.attrReal);
        attrType[2] = new AttrType(AttrType.attrReal);
        attrType[3] = new AttrType(AttrType.attrReal);
        attrType[4] = new AttrType(AttrType.attrReal);

        short[] attrSize = new short[5];
        attrSize[0] = REC_LEN1;
        attrSize[1] = REC_LEN2;
        attrSize[2] = REC_LEN3;
        attrSize[3] = REC_LEN4;
        attrSize[4] = REC_LEN5;



        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 5, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // Create unsorted data file "test1.in"
        RID             rid;
        Heapfile        f = null;
        try {
            f = new Heapfile("test1sortPref.in");
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 5, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        for (int i=0; i<NUM_RECORDS; i++) {
            try {
                for(int j=0; j<5; j++)
                    t.setFloFld(j+1, data1[i][j]);
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[5];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);
        projlist[4] = new FldSpec(rel, 5);

        FileScan fscan = null;

        try {
            fscan = new FileScan("test1sortPref.in", attrType, attrSize, (short) 5, 5, projlist, null);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        // Sort "test1sortPref.in"
        SortPref sort = null;
        try {
            sort = new SortPref(attrType, (short) 5, attrSize, fscan, order[0], new int[]{1,2}, 2, SORTPGNUM);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }


        int count = 0;
        t = null;
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
            if (count >= NUM_RECORDS) {
                System.err.println("Test1 -- OOPS! too many records");
                status = FAIL;
                flag = false;
                break;
            }

            try {
                outval[0] = t.getFloFld(1);
                outval[1] = t.getFloFld(2);
                outval[2] = t.getFloFld(3);
                outval[3] = t.getFloFld(4);
                outval[4] = t.getFloFld(5);

                System.out.println("Got row: "+outval[0]+" "+outval[1]+" "+outval[2]+" "+outval[3]+" "+outval[4]+" ");
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            if (!Arrays.equals(outval, data2[count])) {
                System.err.println("outval = " + outval[0] + "\tdata2[count] = " + data2[count][0]);

                System.err.println("Test1 -- OOPS! test1.out not sorted");
                status = FAIL;
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
        if (count < NUM_RECORDS) {
            System.err.println("Test1 -- OOPS! too few records");
            status = FAIL;
        }
        else if (flag && status) {
            System.err.println("Test1 -- Sorting OK");
        }

        // clean up
        try {
            sort.close();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        System.err.println("------------------- TEST 1 completed ---------------------\n");

        return status;
    }


    protected boolean test2()
    {
        System.out.println("------------------------ TEST 2 --------------------------");

        boolean status = OK;

        AttrType[] attrType = new AttrType[5];
        attrType[0] = new AttrType(AttrType.attrReal);
        attrType[1] = new AttrType(AttrType.attrReal);
        attrType[2] = new AttrType(AttrType.attrReal);
        attrType[3] = new AttrType(AttrType.attrReal);
        attrType[4] = new AttrType(AttrType.attrReal);

        short[] attrSize = new short[5];
        attrSize[0] = REC_LEN1;
        attrSize[1] = REC_LEN2;
        attrSize[2] = REC_LEN3;
        attrSize[3] = REC_LEN4;
        attrSize[4] = REC_LEN5;



        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 5, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // Create unsorted data file "test1.in"
        RID             rid;
        Heapfile        f = null;
        try {
            f = new Heapfile("test2sortPref.in");
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 5, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        for (int i=0; i<NUM_RECORDS; i++) {
            try {
                for(int j=0; j<5; j++)
                    t.setFloFld(j+1, data1[i][j]);
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[5];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);
        projlist[4] = new FldSpec(rel, 5);

        FileScan fscan = null;

        try {
            fscan = new FileScan("test2sortPref.in", attrType, attrSize, (short) 5, 5, projlist, null);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        // Sort "test1sortPref.in"
        SortPref sort = null;
        try {
            sort = new SortPref(attrType, (short) 5, attrSize, fscan, order[1], new int[]{1,2}, 2, SORTPGNUM);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }


        int count = 0;
        t = null;
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
            if (count >= NUM_RECORDS) {
                System.err.println("Test2 -- OOPS! too many records");
                status = FAIL;
                flag = false;
                break;
            }

            try {
                outval[0] = t.getFloFld(1);
                outval[1] = t.getFloFld(2);
                outval[2] = t.getFloFld(3);
                outval[3] = t.getFloFld(4);
                outval[4] = t.getFloFld(5);

                System.out.println("Got row: "+outval[0]+" "+outval[1]+" "+outval[2]+" "+outval[3]+" "+outval[4]+" ");
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            if (!Arrays.equals(outval, data3[count])) {
                System.err.println("outval = " + outval[0] + "\tdata3[count] = " + data3[count][0]);

                System.err.println("Test2 -- OOPS! test2.out not sorted");
                status = FAIL;
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
        if (count < NUM_RECORDS) {
            System.err.println("Test2 -- OOPS! too few records");
            status = FAIL;
        }
        else if (flag && status) {
            System.err.println("Test2 -- Sorting OK");
        }

        // clean up
        try {
            sort.close();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        System.err.println("------------------- TEST 2 completed ---------------------\n");


        return status;
    }

//    protected boolean test3()
//    {
//        System.out.println("------------------------ TEST 3 --------------------------");
//
//        boolean status = OK;
//
//        AttrType[] attrType = new AttrType[6];
//        attrType[0] = new AttrType(AttrType.attrReal);
//        attrType[1] = new AttrType(AttrType.attrReal);
//        attrType[2] = new AttrType(AttrType.attrReal);
//        attrType[3] = new AttrType(AttrType.attrReal);
//        attrType[4] = new AttrType(AttrType.attrReal);
//        attrType[5] = new AttrType(AttrType.attrReal);
//
//        short[] attrSize = new short[6];
//        attrSize[0] = REC_LEN1;
//        attrSize[1] = REC_LEN2;
//        attrSize[2] = REC_LEN3;
//        attrSize[3] = REC_LEN4;
//        attrSize[4] = REC_LEN5;
//        attrSize[5] = REC_LEN5;
//
//
//
//        // create a tuple of appropriate size
//        Tuple t = new Tuple();
//        try {
//            t.setHdr((short) 6, attrType, attrSize);
//        }
//        catch (Exception e) {
//            status = FAIL;
//            e.printStackTrace();
//        }
//
//        int size = t.size();
//
//        // Create unsorted data file "test1.in"
//        RID             rid;
//        Heapfile        f = null;
//        try {
//            f = new Heapfile("test3sortPref.in");
//        }
//        catch (Exception e) {
//            status = FAIL;
//            e.printStackTrace();
//        }
//
//        t = new Tuple(size);
//        try {
//            t.setHdr((short) 6, attrType, attrSize);
//        }
//        catch (Exception e) {
//            status = FAIL;
//            e.printStackTrace();
//        }
//
//        for (int i=0; i<NUM_RECORDS; i++) {
//            try {
//                for(int j=0; j<6; j++)
//                    t.setFloFld(j+1, data4[i][j]);
//            }
//            catch (Exception e) {
//                status = FAIL;
//                e.printStackTrace();
//            }
//
//            try {
//                rid = f.insertRecord(t.returnTupleByteArray());
//            }
//            catch (Exception e) {
//                status = FAIL;
//                e.printStackTrace();
//            }
//        }
//
//        // create an iterator by open a file scan
//        FldSpec[] projlist = new FldSpec[1];
//        RelSpec rel = new RelSpec(RelSpec.outer);
//        projlist[0] = new FldSpec(rel, 6);
//
//
//        FileScan fscan = null;
//
//        try {
//            fscan = new FileScan("test3sortPref.in", attrType, attrSize, (short) 5, 1, projlist, null);
//        }
//        catch (Exception e) {
//            status = FAIL;
//            e.printStackTrace();
//        }
//
//        // Sort "test1sortPref.in"
//        SortPref sort = null;
//        try {
//            sort = new Sort(attrType, (short) 5, null, fscan, order[0], new int[]{1,2,3}, 3, SORTPGNUM);
//        }
//        catch (Exception e) {
//            status = FAIL;
//            e.printStackTrace();
//        }
//
//        int count = 0;
//        t = null;
//        float[] outval = new float[5];
//
//        try {
//            t = sort.get_next();
//        }
//        catch (Exception e) {
//            status = FAIL;
//            e.printStackTrace();
//        }
//
//        boolean flag = true;
//
//        while (t != null) {
//            if (count >= NUM_RECORDS) {
//                System.err.println("Test3 -- OOPS! too many records");
//                status = FAIL;
//                flag = false;
//                break;
//            }
//
//            try {
//                outval[0] = t.getFloFld(1);
//                outval[1] = t.getFloFld(2);
//                outval[2] = t.getFloFld(3);
//                outval[3] = t.getFloFld(4);
//                outval[4] = t.getFloFld(5);
//
//                System.out.println("Got row: "+outval[0]+" "+outval[1]+" "+outval[2]+" "+outval[3]+" "+outval[4]+" | " + (outval[0]+outval[1]+outval[2]+outval[3]+outval[4]));
//            }
//            catch (Exception e) {
//                status = FAIL;
//                e.printStackTrace();
//            }
//
//            if (!Arrays.equals(outval, data5[count])) {
//                System.err.println("outval = " + outval[0] + "\tdata5[count] = " + data5[count][0]);
//
//                System.err.println("Test3 -- OOPS! test3.out not sorted");
//                status = FAIL;
//            }
//            count++;
//
//            try {
//                t = sort.get_next();
//            }
//            catch (Exception e) {
//                status = FAIL;
//                e.printStackTrace();
//            }
//        }
//        if (count < NUM_RECORDS) {
//            System.err.println("Test3 -- OOPS! too few records");
//            status = FAIL;
//        }
//        else if (flag && status) {
//            System.err.println("Test3 -- Sorting OK");
//        }
//
//        // clean up
//        try {
//            sort.close();
//        }
//        catch (Exception e) {
//            status = FAIL;
//            e.printStackTrace();
//        }
//
//        System.err.println("------------------- TEST 3 completed ---------------------\n");
//
//        return status;
//    }

    protected boolean test4()
    {
        System.out.println("------------------------ TEST 4 --------------------------");

        boolean status = OK;

        AttrType[] attrType = new AttrType[5];
        attrType[0] = new AttrType(AttrType.attrReal);
        attrType[1] = new AttrType(AttrType.attrReal);
        attrType[2] = new AttrType(AttrType.attrReal);
        attrType[3] = new AttrType(AttrType.attrReal);
        attrType[4] = new AttrType(AttrType.attrReal);

        short[] attrSize = new short[5];
        attrSize[0] = REC_LEN1;
        attrSize[1] = REC_LEN2;
        attrSize[2] = REC_LEN3;
        attrSize[3] = REC_LEN4;
        attrSize[4] = REC_LEN5;

        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 5, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // Create unsorted data file "test1.in"
        RID             rid;
        Heapfile        f = null;
        try {
            f = new Heapfile("test4sortPref.in");
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 5, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        for (int i=0; i<NUM_RECORDS; i++) {
            try {
                for(int j=0; j<5; j++)
                    t.setFloFld(j+1, data4[i][j]);
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }
        int num_attributes = 5;
        int actual_pref_list[] = {1,2,3};

        FldSpec[] projlist = new FldSpec[num_attributes+1];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for(int i=0;i<num_attributes;i++) projlist[i] = new FldSpec(rel, i+1);
        projlist[num_attributes] = new FldSpec(rel, 1);;

        AttrType[] attrType_for_proj = new AttrType[num_attributes];
        for(int i=0;i<num_attributes;i++) attrType_for_proj[i] = new AttrType(AttrType.attrReal);

        OurFileScan fscan = null;

        try {
            fscan = new OurFileScan("test4sortPref.in", attrType_for_proj, null, (short) num_attributes, num_attributes, projlist, null, actual_pref_list);
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
        t = null;
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
            if (count >= NUM_RECORDS) {
                System.err.println("Test4 -- OOPS! too many records");
                status = FAIL;
                flag = false;
                break;
            }

            try {
                outval[0] = t.getFloFld(1);
                outval[1] = t.getFloFld(2);
                outval[2] = t.getFloFld(3);
                outval[3] = t.getFloFld(4);
                outval[4] = t.getFloFld(5);

                System.out.println("Got row: "+outval[0]+" "+outval[1]+" "+outval[2]+" "+outval[3]+" "+outval[4]+" ");
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            if (!Arrays.equals(outval, data6[count])) {
                System.err.println("outval = " + outval[0] + "\tdata6[count] = " + data6[count][0]);

                System.err.println("Test4 -- OOPS! test4.out not sorted");
                status = FAIL;
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
        if (count < NUM_RECORDS) {
            System.err.println("Test4 -- OOPS! too few records");
            status = FAIL;
        }
        else if (flag && status) {
            System.err.println("Test4 -- Sorting OK");
        }

        // clean up
        try {
            sort.close();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        System.err.println("------------------- TEST 4 completed ---------------------\n");
        return status;
    }

    protected boolean test3() {
        System.out.println("------------------------ TEST 3 --------------------------");

        boolean status = OK;

        AttrType[] attrType = new AttrType[5];
        attrType[0] = new AttrType(AttrType.attrReal);
        attrType[1] = new AttrType(AttrType.attrReal);
        attrType[2] = new AttrType(AttrType.attrReal);
        attrType[3] = new AttrType(AttrType.attrReal);
        attrType[4] = new AttrType(AttrType.attrReal);

        short[] attrSize = new short[5];
        attrSize[0] = REC_LEN1;
        attrSize[1] = REC_LEN2;
        attrSize[2] = REC_LEN3;
        attrSize[3] = REC_LEN4;
        attrSize[4] = REC_LEN5;

        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 5, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // Create unsorted data file "test1.in"
        RID             rid;
        Heapfile        f = null;
        try {
            f = new Heapfile("test3sortPref.in");
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 5, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        for (int i=0; i<NUM_RECORDS; i++) {
            try {
                for(int j=0; j<5; j++)
                    t.setFloFld(j+1, data4[i][j]);
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }


        int pref_list[] = {1,2,3,4,5};
        int actual_pref_list[] = {1,2,3};
        int pref_count = pref_list.length;

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[pref_count+1];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for(int i=0;i<pref_count;i++) projlist[i] = new FldSpec(rel, pref_list[i]);
        projlist[pref_count] = new FldSpec(rel, 1);;

        AttrType[] attrType_for_proj = new AttrType[pref_count];
        for(int i=0;i<pref_count;i++) attrType_for_proj[i] = new AttrType(AttrType.attrReal);


//        FldSpec[] projlist = new FldSpec[6];
//        RelSpec rel = new RelSpec(RelSpec.outer);
//        projlist[0] = new FldSpec(rel, 1);
//        projlist[1] = new FldSpec(rel, 2);
//        projlist[2] = new FldSpec(rel, 3);
//        projlist[3] = new FldSpec(rel, 4);
//        projlist[4] = new FldSpec(rel, 5);
//        projlist[5] = new FldSpec(rel, 5);

        OurFileScan fscan = null;
        try {
            fscan = new OurFileScan("test3sortPref.in", attrType_for_proj, null, (short) pref_count, pref_count, projlist, null, actual_pref_list);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        AttrType[] attrType_for_sort = new AttrType[pref_count+1];
        for(int i=0;i<pref_count;i++) {
            attrType_for_sort[i] = new AttrType(AttrType.attrReal);
        }
        attrType_for_sort[pref_count] = new AttrType(AttrType.attrReal);

//        AttrType[] attrType_for_sort = new AttrType[6];//pref_count];
//        attrType_for_sort[0] = new AttrType(AttrType.attrReal);
//        attrType_for_sort[1] = new AttrType(AttrType.attrReal);
//        attrType_for_sort[2] = new AttrType(AttrType.attrReal);
//        attrType_for_sort[3] = new AttrType(AttrType.attrReal);
//        attrType_for_sort[4] = new AttrType(AttrType.attrReal);
//        attrType_for_sort[5] = new AttrType(AttrType.attrReal);

        Sort sort = null;
        try {
            sort = new Sort(attrType_for_sort, (short) (pref_count+1), null, fscan, pref_count+1, order[0], REC_LEN1, SORTPGNUM);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int count = 0;
        t = null;
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
            if (count >= NUM_RECORDS) {
                System.err.println("Test3 -- OOPS! too many records");
                status = FAIL;
                flag = false;
                break;
            }

            try {
                outval[0] = t.getFloFld(1);
                outval[1] = t.getFloFld(2);
                outval[2] = t.getFloFld(3);
                outval[3] = t.getFloFld(4);
                outval[4] = t.getFloFld(5);
                float x = t.getFloFld(6);
//                outval[4] = 0;
                System.out.println("Got row: "+outval[0]+" "+outval[1]+" "+outval[2]+" "+outval[3]+" "+outval[4]+" "+" | " +x);
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            if (!Arrays.equals(outval, data5[count])) {
                System.err.println("outval = " + outval[0] + "\tdata5[count] = " + data5[count][0]);

                System.err.println("Test3 -- OOPS! test3.out not sorted");
                status = FAIL;
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
        if (count < NUM_RECORDS) {
            System.err.println("Test3 -- OOPS! too few records");
            status = FAIL;
        }
        else if (flag && status) {
            System.err.println("Test3 -- Sorting OK");
        }

        // clean up
        try {
            sort.close();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        System.err.println("------------------- TEST 3 completed ---------------------\n");

        return status;
    }

    protected  boolean test5()
    {
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
                outval[4] = 0;//t.getFloFld(5);

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
        return true;
    }

    protected String testName()
    {
        return "SortPref";
    }
}

public class SortPrefTest
{
    public static void main(String argv[]) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        boolean sortstatus;

        SORTPrefDriver sortt = new SORTPrefDriver();

        sortstatus = sortt.runTests();
        if (sortstatus != true) {
            System.out.println("Error ocurred during sorting tests");
        }
        else {
            System.out.println("Sorting tests completed successfully");
        }
    }
}

