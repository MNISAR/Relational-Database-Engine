package tests;

import java.io.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import hashindex.HIndex;
import hashindex.HashKey;
import heap.*;
import iterator.*;
import index.*;
import btree.*;
import java.util.Random;
import java.util.Scanner;


class IndexHashDriver extends TestDriver
        implements GlobalConst {

    public IndexHashDriver() {
        super("indexhashtest");
    }

    public boolean runTests ()  {

        System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");

        SystemDefs sysdef = new SystemDefs( dbpath, 500, NUMBUF, "Clock" );

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
        boolean _pass = false;
        try {
            _pass = runAllTests();
        } catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

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

        AttrType[] attrType = new AttrType[3];
        attrType[0] = new AttrType(AttrType.attrInteger);
        attrType[1] = new AttrType(AttrType.attrString);
        attrType[2] = new AttrType(AttrType.attrReal);
        short[] attrSize = new short[1];
        attrSize[0] = 15;

        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) attrType.length, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // Create unsorted data file "test1.in"
        RID             rid;
        Heapfile        f = null;
        HIndex h1=null,h2=null,h3=null;
        try {
            f = new Heapfile("test1.in");
            h1 = new HIndex("hash_index1", AttrType.attrInteger, 4,50);
            h2 = new HIndex("hash_index2", AttrType.attrString, 15,50);
            h3 = new HIndex("hash_index3", AttrType.attrReal, 4,50);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) attrType.length, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        for (int i=0; i<10; i++) {
            try {
                i = i %10;
                t.setIntFld(1, i);
                t.setStrFld(2, String.valueOf(i));
                t.setFloFld(3, (float) i );
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.getTupleByteArray());
                h1.insert(new HashKey(t.getIntFld(1)), rid);
                h2.insert(new HashKey(t.getStrFld(2)), rid);
                h3.insert(new HashKey(t.getFloFld(3)), rid);
                rid = f.insertRecord(t.getTupleByteArray());
                h1.insert(new HashKey(t.getIntFld(1)), rid);
                h2.insert(new HashKey(t.getStrFld(2)), rid);
                h3.insert(new HashKey(t.getFloFld(3)), rid);
                rid = f.insertRecord(t.getTupleByteArray());
                h1.insert(new HashKey(t.getIntFld(1)), rid);
                h2.insert(new HashKey(t.getStrFld(2)), rid);
                h3.insert(new HashKey(t.getFloFld(3)), rid);
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        System.out.println("Hash Search using int");
        // hash search with key on int: 3
        try {
            FldSpec[] projlist = new FldSpec[3];
            RelSpec rel = new RelSpec(RelSpec.outer);
            projlist[0] = new FldSpec(rel, 1);
            projlist[1] = new FldSpec(rel, 2);
            projlist[2] = new FldSpec(rel, 3);

            CondExpr[] expr = {new CondExpr(), new CondExpr()};
            expr[0].next = null;
            expr[0].op = new AttrOperator(AttrOperator.aopEQ);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            expr[0].type2 = new AttrType(AttrType.attrInteger);
            expr[0].operand2.integer = 3;
            expr[1] = null;

            IndexScan iscan = null;

            iscan = new IndexScan(new IndexType(IndexType.Hash), "test1.in", "hash_index1",
                                attrType, attrSize, attrType.length,
                                projlist.length, projlist, expr,
                                1, true);
            t = null;
            while ((t=iscan.get_next()) != null) {
                System.out.println("Tuple: " + t.getIntFld(1) + " | " + t.getStrFld(2) + " | " +t.getFloFld(3) );
            }
            iscan.close();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        System.out.println("Hash Search using string");
        // hash search with key on str: Str :3
        try {
            FldSpec[] projlist = new FldSpec[3];
            RelSpec rel = new RelSpec(RelSpec.outer);
            projlist[0] = new FldSpec(rel, 1);
            projlist[1] = new FldSpec(rel, 2);
            projlist[2] = new FldSpec(rel, 3);

            CondExpr[] expr = {new CondExpr(), new CondExpr()};
            expr[0].next = null;
            expr[0].op = new AttrOperator(AttrOperator.aopEQ);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
            expr[0].type2 = new AttrType(AttrType.attrString);
            expr[0].operand2.string = String.valueOf(3);
            expr[1] = null;

            IndexScan iscan = null;

            iscan = new IndexScan(new IndexType(IndexType.Hash), "test1.in", "hash_index2",
                    attrType, attrSize, attrType.length,
                    projlist.length, projlist, expr,
                    1, true);
            t = null;
            while ((t=iscan.get_next()) != null) {
                System.out.println("Tuple: " + t.getIntFld(1) + " | " + t.getStrFld(2) + " | " +t.getFloFld(3) );
            }
            iscan.close();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        System.out.println("Hash Search using float");
        // hash search with key on float: 3.0
        try {
            FldSpec[] projlist = new FldSpec[3];
            RelSpec rel = new RelSpec(RelSpec.outer);
            projlist[0] = new FldSpec(rel, 1);
            projlist[1] = new FldSpec(rel, 2);
            projlist[2] = new FldSpec(rel, 3);

            CondExpr[] expr = {new CondExpr(), new CondExpr()};
            expr[0].next = null;
            expr[0].op = new AttrOperator(AttrOperator.aopEQ);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
            expr[0].type2 = new AttrType(AttrType.attrReal);
            expr[0].operand2.real = (float) 3.0;
            expr[1] = null;

            IndexScan iscan = null;

            iscan = new IndexScan(new IndexType(IndexType.Hash), "test1.in", "hash_index3",
                    attrType, attrSize, attrType.length,
                    projlist.length, projlist, expr,
                    1, true);
            t = null;
            while ((t=iscan.get_next()) != null) {
                System.out.println("Tuple: " + t.getIntFld(1) + " | " + t.getStrFld(2) + " | " +t.getFloFld(3) );
            }
            iscan.close();
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
        return true;
    }


    protected boolean test3()
    {
        return true;
    }

    protected boolean test4()
    {
        return true;
    }

    protected boolean test5()
    {
        return true;
    }

    protected boolean test6()
    {
        return true;
    }

    protected String testName()
    {
        return "Index";
    }
}

public class IndexHashTest
{
    public static void main(String argv[])
    {
        boolean indexstatus;

        IndexHashDriver indext = new IndexHashDriver();

        indexstatus = indext.runTests();
        if (indexstatus != true) {
            System.out.println("Error ocurred during index tests");
        }
        else {
            System.out.println("Index tests completed successfully");
        }
    }
}

