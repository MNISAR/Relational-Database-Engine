package tests;
//originally from : joins.C

import hashindex.HIndex;
import iterator.*;
import heap.*;
import global.*;
import index.*;

import java.io.*;
import java.util.*;
import java.lang.*;

import btree.*;

/**
 * Here is the implementation for the tests. There are N tests performed.
 * We start off by showing that each operator works on its own.
 * Then more complicated trees are constructed.
 * As a nice feature, we allow the user to specify a selection condition.
 * We also allow the user to hardwire trees together.
 */

//Define the Reserves schema
class Data1{
    public int sid;
    public int bid;
    public float cid;
    public String date;
    public Data1(int _sid, String _date, int _bid, float _cid) {
        sid = _sid;
        bid = _bid;
        date = _date;
        cid = _cid;
    }
}
class Data2{
    public int sid;
    public int bid;
    public String date;
    public Data2(int _sid, int _bid, String _date) {
        sid = _sid;
        bid = _bid;
        date = _date;
    }
}

class HashJoinsDriver extends TestDriver
implements GlobalConst {

    private boolean OK = true;
    private boolean FAIL = false;
    private Vector sailors;
    private Vector boats;
    private Vector reserves;
    private BTreeFile btree_index;
    /**
     * Constructor
     */
    private static int   LARGE = 1000;
    private static short REC_LEN1 = 32;
    private static short REC_LEN2 = 160;
    private static int   SORTPGNUM = 12;

    protected String testName()
    {
        return "HashJoin";
    }

    public HashJoinsDriver() {

        String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase.jointestdb";
        String logpath = "/tmp/" + System.getProperty("user.name") + ".joinlog";

        String remove_cmd = "/bin/rm -rf ";
        String remove_logcmd = remove_cmd + logpath;
        String remove_dbcmd = remove_cmd + dbpath;
        String remove_joincmd = remove_cmd + dbpath;

        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
            Runtime.getRuntime().exec(remove_joincmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }
        SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");
    }

    public boolean runAllTests() {

        Query7();
        System.out.print("Finished joins testing" + "\n");


        return true;
    }

    private void Query7_CondExpr_int(CondExpr[] expr) {
        expr[0] = new CondExpr();
        expr[0].next = null;
        expr[0].op = new AttrOperator(AttrOperator.aopEQ);

        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);

        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);

        expr[1] = null;
    }
    private void Query7_CondExpr_str(CondExpr[] expr) {
        expr[0] = new CondExpr();
        expr[0].next = null;
        expr[0].op = new AttrOperator(AttrOperator.aopEQ);

        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);

        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 3);

        expr[1] = null;
    }

    public void Query7() {
        System.out.print("**********************Query7 strating *********************\n");
        boolean status = OK;
        System.out.print("Query: Join sailors with reserves on id\n"
                + "  SELECT   S.sname, S.sid\n"
                + "  JOIN Sailors'S' 1  Reserves'R' 1 = \n\n\n");

        CondExpr[] outFilter = new CondExpr[2];

        Tuple t = new Tuple();

        String outer_relation_name = "test1_o.in";
        String inner_relation_name = "test1_i.in";

        Data1[] data1 = {
                // int , str, int , floaat
                new Data1(1, "Kushal", 1, (float) 1),
                new Data1(3, "Krupal", 1, (float) 1),
                new Data1(4, "Pooja", 1, (float) 1),
                new Data1(5, "Monil", 1, (float) 1)
        };
        Data2[] data2 = {
                // int, int ,str
            new Data2(1, 1, "Monil"),
            new Data2(2, 1, "Samip")
        };

        AttrType[] Stypes = {
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrString),
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrReal)
        };
        short[] Ssizes = new short[1];
        Ssizes[0] = 30;

        // creating outer relation's heap file
        try{
            Heapfile hf = new Heapfile(outer_relation_name);
            t.setHdr((short)Stypes.length, Stypes, Ssizes);
            int size = t.size();
            t = new Tuple(size);
            t.setHdr((short)Stypes.length, Stypes, Ssizes);
            for(int i=0;i<data1.length;i++){
                t.setIntFld(1, data1[i].sid);
                t.setStrFld(2, "f:"+data1[i].date);
                t.setIntFld(3, data1[i].bid);
                t.setFloFld(4, data1[i].cid);

                hf.insertRecord(t.getTupleByteArray());
            }
            System.out.println("Outer Heap file created. " + hf.getRecCnt() + " Records");
        }catch (Exception e){
            e.printStackTrace();
        }


        AttrType[] Rtypes = {
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrString),
        };
        short[] Rsizes = new short[1];
        Rsizes[0] = 15;
        // creating inner relation's heap file
        try{
            Heapfile hf2 = new Heapfile(inner_relation_name);
            t.setHdr((short)Rtypes.length, Rtypes, Rsizes);
            int size = t.size();
            t = new Tuple(size);
            t.setHdr((short)Rtypes.length, Rtypes, Rsizes);
            for(int i=0;i<data2.length;i++){
                t.setIntFld(1, data2[i].sid);
                t.setIntFld(2, data2[i].bid);
                t.setStrFld(3, "f:"+data2[i].date);

                hf2.insertRecord(t.getTupleByteArray());
            }
            System.out.println("Inner Heap file created."+ hf2.getRecCnt() + " Records");
        }catch (Exception e){
            e.printStackTrace();
        }

        short[] JJsize = new short[1];
        JJsize[0] = 30;

        FldSpec[] proj1 = {
                new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.outer), 2),
                new FldSpec(new RelSpec(RelSpec.outer), 3),
                new FldSpec(new RelSpec(RelSpec.outer), 4),
                new FldSpec(new RelSpec(RelSpec.innerRel), 1),
                new FldSpec(new RelSpec(RelSpec.innerRel), 2),
                new FldSpec(new RelSpec(RelSpec.innerRel), 3)
        };

        FldSpec[] Sprojection = {
                new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.outer), 2),
                new FldSpec(new RelSpec(RelSpec.outer), 3),
                new FldSpec(new RelSpec(RelSpec.outer), 4)
        };

        try {
            int count = 0;
            System.out.println("\nJoin on integer attribute.");
            Query7_CondExpr_int(outFilter);
            FileScan am = null;
            am = new FileScan(outer_relation_name, Stypes, Ssizes,
                    (short) Stypes.length, (short) Sprojection.length, Sprojection, null);
            HashJoin inl = null;

            inl = new HashJoin(Stypes, Stypes.length, Ssizes,
                    Rtypes, Rtypes.length, Rsizes,
                    50,
                    am, inner_relation_name,
                    outFilter, null, proj1, proj1.length);
            AttrType [] JJtype = {
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrString),
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrReal),
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrString)
            };
            int c = 0;
            while ((t = inl.get_next()) != null) {
                count++;
                t.print(JJtype);
            }
            am.close();
            System.out.println("total "+count+" tuples in the result.");
            inl.close();
        }catch (Exception e) {
            System.err.println("*** Error preparing for nested_loop_join");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        System.out.println("\n\n\nIndex");
        try {
            HIndex h = new HIndex("hash-join-inner-index.unclustered");
            h.printBucketInfo();
        }catch (Exception e){
            e.printStackTrace();
        }

        try {
            int count = 0;
            System.out.println("\n\nJoin on String attribute.");
            Query7_CondExpr_str(outFilter);
            FileScan am = null;
            am = new FileScan(outer_relation_name, Stypes, Ssizes,
                    (short) Stypes.length, (short) Sprojection.length, Sprojection, null);
            HashJoin inl = null;
            inl = new HashJoin(Stypes, Stypes.length, Ssizes,
                    Rtypes, Rtypes.length, Rsizes,
                    50,
                    am, inner_relation_name,
                    outFilter, null, proj1, proj1.length);
            AttrType [] JJtype = {
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrString),
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrReal),
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrString)
            };
            int c = 0;
            while ((t = inl.get_next()) != null) {
                count ++;
                t.print(JJtype);
            }
            am.close();
            System.out.println("total "+count+" tuples in the result.");
            inl.close();
        }catch (Exception e) {
            System.err.println("*** Error preparing for nested_loop_join");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        System.out.println("\n\n\nAfter Index(will have inserts from previous INLJ as well. We can ignore them.)");
        try {
            HIndex h = new HIndex("hash-join-inner-index.unclustered");
            h.printBucketInfo();
        }catch (Exception e){
            e.printStackTrace();
        }



        Runtime.getRuntime().exit(1);
    }
}


public class HashJoinTest {
    public static void main(String argv[]) {
        boolean sortstatus=false;
//        SystemDefs global = new SystemDefs("bingjiedb", 100, 70, null);
//        JavabaseDB.openDB("/tmp/nwangdb", 5000);

        HashJoinsDriver jjoin = new HashJoinsDriver();

        try{
            sortstatus = jjoin.runAllTests();
        }catch (Exception e){
            e.printStackTrace();
        }
        if (sortstatus != true) {
            System.out.println("Error ocurred during join tests");
        } else {
            System.out.println("join tests completed successfully");
        }
    }
}