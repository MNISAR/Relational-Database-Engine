package tests;

import java.io.*;

import bufmgr.PageNotReadException;
import diskmgr.PCounter;
import diskmgr.Page;
import global.*;
import heap.*;
import index.IndexException;
import iterator.*;

import java.util.Arrays;
import java.util.Scanner;



class NestedLoopsSkyDriver extends TestDriver
        implements GlobalConst {

    private static short REC_LEN1 = 32;
    private static short REC_LEN2 = 32;
    private static short REC_LEN3 = 32;
    private static short REC_LEN4 = 32;
    private static short REC_LEN5 = 32;
    private static NestedLoopsSky it;
    private static BlockNestedLoopsSky blck_it;

    TupleOrder[] order = new TupleOrder[2];

    public NestedLoopsSkyDriver() {
        super("nestedloopskytest");
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);
    }

    public boolean runTests () throws HFDiskMgrException, HFException, HFBufMgrException, IOException {

        System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");
        // We will define the bufpoolsize and num_pgs params ; whereas BUFF_SIZE determined by user input
        SystemDefs sysdef = new SystemDefs( dbpath, 8000, 100, "Clock" );

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
        /*try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
        }*/

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        //This step seems redundant for me.  But it's in the original
        //C++ code.  So I am keeping it as of now, just in case I
        //I missed something
        /*try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
        }*/

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();

        //Clean up again
        /*try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
        }*/

        System.out.println ("\n" + "..." + testName() + " tests ");
        System.out.println (_pass==OK ? "completely successfully" : "failed");
        System.out.println (".\n\n");

        return _pass;
    }

    protected boolean test1()
    {
        System.out.println("------------------------ TEST 1 --------------------------");

        boolean status = OK;

        // Read data and construct tuples
        String relationName = "data/data3.txt";
        File file = new File(relationName);
        Scanner sc = null;
        try {
            sc = new Scanner(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        int COLS = sc.nextInt();

        AttrType[] attrType = new AttrType[COLS];
        attrType[0] = new AttrType(AttrType.attrReal);
        attrType[1] = new AttrType(AttrType.attrReal);
        attrType[2] = new AttrType(AttrType.attrReal);
        attrType[3] = new AttrType(AttrType.attrReal);
        attrType[4] = new AttrType(AttrType.attrReal);

        short[] attrSize = new short[COLS];
        attrSize[0] = REC_LEN1;
        attrSize[1] = REC_LEN2;
        attrSize[2] = REC_LEN3;
        attrSize[3] = REC_LEN4;
        attrSize[4] = REC_LEN5;

        String hfileName = "test1nestedloopsky.in";


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
                //t.print(attrType);
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

        }

        try {
        	System.out.println("Number of Disk reads: "+ PCounter.get_rcounter());
            System.out.println("Number of Disk writes: "+ PCounter.get_wcounter());
            //boolean status = OK;
            
            it = new NestedLoopsSky(attrType,
                    (short)COLS,
                    attrSize,
                    null,
                    hfileName,
                    new int[]{1,2},
                    2,
                    5);

            System.out.println("Printing the Nested Loop Skyline");
            Tuple temp = it.get_next();
            while (temp!=null)
            {
            	temp.print(attrType);
            	temp = it.get_next();
            }
            System.out.println("Number of Disk reads: "+ PCounter.get_rcounter());
            System.out.println("Number of Disk writes: "+ PCounter.get_wcounter());
            //System.out.println("size of iterator: " + ObjectSizeFetcher.getObjectSize(it));
        } catch (FileScanException e) {
            e.printStackTrace();
        } catch (TupleUtilsException e) {
            e.printStackTrace();
        } catch (InvalidRelation invalidRelation) {
            invalidRelation.printStackTrace();
        } catch (WrongPermat wrongPermat) {
            wrongPermat.printStackTrace();
        } catch (InvalidTypeException e) {
            e.printStackTrace();
        } catch (JoinsException e) {
            e.printStackTrace();
        } catch (PageNotReadException e) {
            e.printStackTrace();
        } catch (FieldNumberOutOfBoundException e) {
            e.printStackTrace();
        } catch (PredEvalException e) {
            e.printStackTrace();
        } catch (UnknowAttrType unknowAttrType) {
            unknowAttrType.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            sc.close();
            it.close();
        }

        System.out.println("------------------ TEST 1 completed ----------------------");


        return status;
    }


    protected boolean test2() throws IOException {
        System.out.println("------------------------ TEST 2 --------------------------");

        boolean status = OK;

        // Read data and construct tuples
        File file = new File("data/data3.txt");
        Scanner sc = new Scanner(file);

        int COLS = sc.nextInt();

        AttrType[] attrType = new AttrType[COLS];
        attrType[0] = new AttrType(AttrType.attrReal);
        attrType[1] = new AttrType(AttrType.attrReal);
        attrType[2] = new AttrType(AttrType.attrReal);
        attrType[3] = new AttrType(AttrType.attrReal);
        attrType[4] = new AttrType(AttrType.attrReal);

        short[] attrSize = new short[COLS];
        attrSize[0] = REC_LEN1;
        attrSize[1] = REC_LEN2;
        attrSize[2] = REC_LEN3;
        attrSize[3] = REC_LEN4;
        attrSize[4] = REC_LEN5;

        String hfileName = "test2nestedloopsky.in";


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
                //t.print(attrType);
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

        }

        try {
        	System.out.println("Number of Disk reads: "+ PCounter.get_rcounter());
            System.out.println("Number of Disk writes: "+ PCounter.get_wcounter());
        	blck_it = new BlockNestedLoopsSky(attrType,
                    						  (short)COLS,
                    						  attrSize,
                    						  null,
                    						  hfileName,
                    						  new int[]{1,2},
                    						  2,
                    						  1);
            System.out.println("Printing the Block Nested Loop Skyline");
            Tuple temp;
			try {
				temp = blck_it.get_next();
				while (temp!=null)
	            {
	            	temp.print(attrType);
	            	temp = blck_it.get_next();
	            }
				System.out.println("Number of Disk reads: "+ PCounter.get_rcounter());
	            System.out.println("Number of Disk writes: "+ PCounter.get_wcounter());
				
			} catch (IndexException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SortException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (LowMemException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnknownKeyTypeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            

        } catch (FileScanException e) {
            e.printStackTrace();
        } catch (TupleUtilsException e) {
            e.printStackTrace();
        } catch (InvalidRelation invalidRelation) {
            invalidRelation.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            sc.close();
            it.close();
        }

        System.out.println("------------------ TEST 2 completed ----------------------");


        return status;
    }


    protected String testName()
    {
        return "NestedLoopsSky";
    }
}


public class NestedLoopSkyTest
{
    public static void main(String argv[]) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        boolean sortstatus;

        NestedLoopsSkyDriver driver = new NestedLoopsSkyDriver();

        sortstatus = driver.runTests();
        if (sortstatus != true) {
            System.out.println("Error occurred during nested loop sky tests");
        }
        else {
            System.out.println("Nested Loop sky tests completed successfully");
        }
    }
}


