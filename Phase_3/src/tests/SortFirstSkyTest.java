package tests;

import java.io.*;

import bufmgr.PageNotReadException;
import global.*;
import heap.*;
import index.IndexException;
import iterator.*;

import java.util.Arrays;
import java.util.Scanner;


class SortFirstSkyDriver extends TestDriver
        implements GlobalConst {

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


    private static int   NUM_RECORDS = data1.length;
    private static short REC_LEN = 8;
    private static int   SORTPGNUM = 1500;
    private static int BUFF_SIZE = 10;

    TupleOrder[] order = new TupleOrder[2];

    public SortFirstSkyDriver() {
        super("sortfirstskytest");
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);
    }

    public boolean runTests () throws HFDiskMgrException, HFException, HFBufMgrException, IOException {

        System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");
        // We will define the bufpoolsize and num_pgs params ; whereas BUFF_SIZE determined by user input
        SystemDefs sysdef = new SystemDefs( dbpath, 3000, 6000, "Clock" );

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
        attrSize[0] = REC_LEN;
        attrSize[1] = REC_LEN;
        attrSize[2] = REC_LEN;
        attrSize[3] = REC_LEN;
        attrSize[4] = REC_LEN;


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
            sort = new SortPref(attrType, (short) 5, attrSize, fscan, order[1], new int[]{1,2}, 2, 5);
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
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            if (!Arrays.equals(outval, data2[count])) {
                System.err.println("outval = " + outval[0] + "\tdata2[count] = " + data1[count][0]);

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


    protected boolean test2() throws IOException {
        System.out.println("------------------------ TEST 2 --------------------------");

        boolean status = OK;

        // Read data and construct tuples
        File file = new File("../../data/data3.txt");
        Scanner sc = new Scanner(file);

        int COLS = sc.nextInt();

        AttrType[] attrType = new AttrType[COLS];
        for(int i=0; i<attrType.length; i++){
            attrType[i] = new AttrType(AttrType.attrReal);
        }


        short[] attrSize = new short[COLS];
        for(int i=0; i<attrSize.length; i++){
            attrSize[i] = REC_LEN;
        }

        String hfileName = "test2sortPref.in";

        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) COLS, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // Create unsorted data file "test1.in"
        RID             rid = null;
        Heapfile        f = null;
        try {
            f = new Heapfile(hfileName);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) COLS, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int t_size = t.size();

        while (sc.hasNextLine()) {
            // create a tuple of appropriate size

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
                    status = FAIL;
                    e.printStackTrace();
                }
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
        FldSpec[] projlist = new FldSpec[COLS];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for(int i=0; i<attrSize.length; i++){
            projlist[i] = new FldSpec(rel, i+1);;
        }

        FileScan fscan = null;

        try {
            fscan = new FileScan(hfileName, attrType, attrSize, (short) COLS, COLS, projlist, null);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        SortFirstSky sortFirstSky = null;
        try {
            sortFirstSky = new SortFirstSky(attrType,
                                            (short) COLS,
                                            attrSize,
                                            fscan,
                                            (short) t_size,
                                            hfileName,
                                            new int[]{1,2,3,4,5},
                                           5,
                                            BUFF_SIZE);

            while(sortFirstSky.get_next() != null) {
                System.out.println("Skyline object: ");
                sortFirstSky.get_next().print(attrType);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (IndexException e) {
            e.printStackTrace();
        } catch (PredEvalException e) {
            e.printStackTrace();
        } catch (UnknowAttrType unknowAttrType) {
            unknowAttrType.printStackTrace();
        } catch (JoinsException e) {
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        } catch (PageNotReadException e) {
            e.printStackTrace();
        } catch (UnknownKeyTypeException e) {
            e.printStackTrace();
        } catch (LowMemException e) {
            e.printStackTrace();
        } catch (InvalidTypeException e) {
            e.printStackTrace();
        } catch (SortException e) {
            e.printStackTrace();
        } catch (TupleUtilsException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            status = OK;
            // clean up

            try {
                f.deleteFile();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        System.err.println("------------------- TEST 2 completed ---------------------\n");

        return status;
    }

    protected boolean test3() throws IOException {
        System.out.println("------------------------ TEST 3 --------------------------");

        boolean status = OK;

        // Read data and construct tuples
        File file = new File("../../data/data_large_skyline.txt");
        Scanner sc = new Scanner(file);

        int COLS = sc.nextInt();

        AttrType[] attrType = new AttrType[COLS];
        for(int i=0; i<attrType.length; i++){
            attrType[i] = new AttrType(AttrType.attrReal);
        }


        short[] attrSize = new short[COLS];
        for(int i=0; i<attrSize.length; i++){
            attrSize[i] = REC_LEN;
        }

        String hfileName = "test3sortFirstSky.in";

        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) COLS, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // Create unsorted data file "test1.in"
        RID             rid = null;
        Heapfile        f = null;
        try {
            f = new Heapfile(hfileName);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) COLS, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int t_size = t.size();

        while (sc.hasNextLine()) {
            // create a tuple of appropriate size

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
                    status = FAIL;
                    e.printStackTrace();
                }
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
        FldSpec[] projlist = new FldSpec[COLS];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for(int i=0; i<attrSize.length; i++){
            projlist[i] = new FldSpec(rel, i+1);;
        }

        FileScan fscan = null;

        try {
            fscan = new FileScan(hfileName, attrType, attrSize, (short) COLS, COLS, projlist, null);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        SortFirstSky sortFirstSky = null;

        try {
            sortFirstSky = new SortFirstSky(attrType,
                    (short) COLS,
                    attrSize,
                    fscan,
                    (short) t_size,
                    hfileName,
                    new int[]{1},
                    1,
                    BUFF_SIZE);

            while(sortFirstSky.hasNext()) {
                System.out.println("Skyline object: ");
                Tuple xt = sortFirstSky.get_next();
                xt.print(attrType);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (IndexException e) {
            e.printStackTrace();
        } catch (PredEvalException e) {
            e.printStackTrace();
        } catch (UnknowAttrType unknowAttrType) {
            unknowAttrType.printStackTrace();
        } catch (JoinsException e) {
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        } catch (PageNotReadException e) {
            e.printStackTrace();
        } catch (UnknownKeyTypeException e) {
            e.printStackTrace();
        } catch (LowMemException e) {
            e.printStackTrace();
        } catch (InvalidTypeException e) {
            e.printStackTrace();
        } catch (SortException e) {
            e.printStackTrace();
        } catch (TupleUtilsException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            status = OK;
            // clean up
            try {
                sortFirstSky.close();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        System.err.println("------------------- TEST 3 completed ---------------------\n");

        return status;
    }


    protected String testName()
    {
        return "SortFirstSky";
    }
}

public class SortFirstSkyTest
{
    public static void main(String argv[]) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        boolean sortstatus;

        SortFirstSkyDriver driver = new SortFirstSkyDriver();

        sortstatus = driver.runTests();
        if (sortstatus != true) {
            System.out.println("Error occurred during sort first sky tests");
        }
        else {
            System.out.println("Sort first sky tests completed successfully");
        }
    }
}


