package tests;
//originally from : joins.C

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

//Define the Sailor schema
class Sailor {
    public int sid;
    public String sname;
    public int rating;
    public double age;

    public Sailor(int _sid, String _sname, int _rating, double _age) {
        sid = _sid;
        sname = _sname;
        rating = _rating;
        age = _age;
    }
}

//Define the Boat schema
class Boats {
    public int bid;
    public String bname;
    public String color;

    public Boats(int _bid, String _bname, String _color) {
        bid = _bid;
        bname = _bname;
        color = _color;
    }
}

//Define the Reserves schema
class Reserves {
    public int sid;
    public int bid;
    public String date;

    public Reserves(int _sid, int _bid, String _date) {
        sid = _sid;
        bid = _bid;
        date = _date;
    }
}

class JoinsDriver implements GlobalConst {

    private boolean OK = true;
    private boolean FAIL = false;
    private Vector sailors;
    private Vector boats;
    private Vector reserves;
    private BTreeFile btree_index;
    /**
     * Constructor
     */
    public JoinsDriver() {

        //build Sailor, Boats, Reserves table
        sailors = new Vector();
        boats = new Vector();
        reserves = new Vector();

        sailors.addElement(new Sailor(53, "Bob Holloway", 9, 53.6));
        sailors.addElement(new Sailor(54, "Susan Horowitz", 1, 34.2));
        sailors.addElement(new Sailor(57, "Yannis Ioannidis", 8, 40.2));
        sailors.addElement(new Sailor(59, "Deborah Joseph", 10, 39.8));
        sailors.addElement(new Sailor(61, "Landwebber", 8, 56.7));
        sailors.addElement(new Sailor(63, "James Larus", 9, 30.3));
        sailors.addElement(new Sailor(64, "Barton Miller", 5, 43.7));
        sailors.addElement(new Sailor(67, "David Parter", 1, 99.9));
        sailors.addElement(new Sailor(69, "Raghu Ramakrishnan", 9, 37.1));
        sailors.addElement(new Sailor(71, "Guri Sohi", 10, 42.1));
        sailors.addElement(new Sailor(73, "Prasoon Tiwari", 8, 39.2));
        sailors.addElement(new Sailor(39, "Anne Condon", 3, 30.3));
        sailors.addElement(new Sailor(47, "Charles Fischer", 6, 46.3));
        sailors.addElement(new Sailor(49, "James Goodman", 4, 50.3));
        sailors.addElement(new Sailor(50, "Mark Hill", 5, 35.2));
        sailors.addElement(new Sailor(75, "Mary Vernon", 7, 43.1));
        sailors.addElement(new Sailor(79, "David Wood", 3, 39.2));
        sailors.addElement(new Sailor(84, "Mark Smucker", 9, 25.3));
        sailors.addElement(new Sailor(87, "Martin Reames", 10, 24.1));
        sailors.addElement(new Sailor(10, "Mike Carey", 9, 40.3));
        sailors.addElement(new Sailor(21, "David Dewitt", 10, 47.2));
        sailors.addElement(new Sailor(29, "Tom Reps", 7, 39.1));
        sailors.addElement(new Sailor(31, "Jeff Naughton", 5, 35.0));
        sailors.addElement(new Sailor(35, "Miron Livny", 7, 37.6));
        sailors.addElement(new Sailor(37, "Marv Solomon", 10, 48.9));

        boats.addElement(new Boats(1, "Onion", "white"));
        boats.addElement(new Boats(2, "Buckey", "red"));
        boats.addElement(new Boats(3, "Enterprise", "blue"));
        boats.addElement(new Boats(4, "Voyager", "green"));
        boats.addElement(new Boats(5, "Wisconsin", "red"));

        reserves.addElement(new Reserves(10, 1, "05/10/95"));
        reserves.addElement(new Reserves(21, 1, "05/11/95"));
        reserves.addElement(new Reserves(10, 2, "05/11/95"));
        reserves.addElement(new Reserves(31, 1, "05/12/95"));
        reserves.addElement(new Reserves(10, 3, "05/13/95"));
        reserves.addElement(new Reserves(69, 4, "05/12/95"));
        reserves.addElement(new Reserves(69, 5, "05/14/95"));
        reserves.addElement(new Reserves(21, 5, "05/16/95"));
        reserves.addElement(new Reserves(57, 2, "05/10/95"));
        reserves.addElement(new Reserves(35, 3, "05/15/95"));

        boolean status = OK;
        int numsailors = 25;
        int numsailors_attrs = 4;
        int numreserves = 10;
        int numreserves_attrs = 3;
        int numboats = 5;
        int numboats_attrs = 3;

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


    /*
    ExtendedSystemDefs extSysDef =
      new ExtendedSystemDefs( "/tmp/minibase.jointestdb", "/tmp/joinlog",
			      1000,500,200,"Clock");
    */

        SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");

        // creating the sailors relation
        AttrType[] Stypes = new AttrType[3];
        Stypes[0] = new AttrType(AttrType.attrInteger);
        Stypes[1] = new AttrType(AttrType.attrString);
        Stypes[2] = new AttrType(AttrType.attrInteger);
//        Stypes[3] = new AttrType(AttrType.attrReal);

        //SOS
        short[] Ssizes = new short[1];
        Ssizes[0] = 30; //first elt. is 30

        Tuple t = new Tuple();
        try {
            t.setHdr((short) 3, Stypes, Ssizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // inserting the tuple into file "sailors"
        RID rid;
        Heapfile f = null;
        try {
            f = new Heapfile("sailors.in");
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 3, Stypes, Ssizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        for (int i = 0; i < numsailors; i++) {
            try {
                t.setIntFld(1, ((Sailor) sailors.elementAt(i)).sid);
                t.setStrFld(2, ((Sailor) sailors.elementAt(i)).sname);
                t.setIntFld(3, ((Sailor) sailors.elementAt(i)).rating);
//                t.setFloFld(4, (float) ((Sailor) sailors.elementAt(i)).age);
            } catch (Exception e) {
                System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            } catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                status = FAIL;
                e.printStackTrace();
            }
        }
        if (status != OK) {
            //bail out
            System.err.println("*** Error creating relation for sailors");
            Runtime.getRuntime().exit(1);
        }

        //creating the boats relation
        AttrType[] Btypes = {
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrString),
                new AttrType(AttrType.attrString),
        };

        short[] Bsizes = new short[2];
        Bsizes[0] = 30;
        Bsizes[1] = 20;
        t = new Tuple();
        try {
            t.setHdr((short) 3, Btypes, Bsizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        size = t.size();

        // inserting the tuple into file "boats"
        //RID             rid;
        f = null;
        try {
            f = new Heapfile("boats.in");
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 3, Btypes, Bsizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        for (int i = 0; i < numboats; i++) {
            try {
                t.setIntFld(1, ((Boats) boats.elementAt(i)).bid);
                t.setStrFld(2, ((Boats) boats.elementAt(i)).bname);
                t.setStrFld(3, ((Boats) boats.elementAt(i)).color);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setStrFld() ***");
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            } catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                status = FAIL;
                e.printStackTrace();
            }
        }
        if (status != OK) {
            //bail out
            System.err.println("*** Error creating relation for boats");
            Runtime.getRuntime().exit(1);
        }

        //creating the boats relation
        AttrType[] Rtypes = new AttrType[3];
        Rtypes[0] = new AttrType(AttrType.attrInteger);
        Rtypes[1] = new AttrType(AttrType.attrInteger);
        Rtypes[2] = new AttrType(AttrType.attrString);

        short[] Rsizes = new short[1];
        Rsizes[0] = 15;
        t = new Tuple();
        try {
            t.setHdr((short) 3, Rtypes, Rsizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        size = t.size();

        // inserting the tuple into file "boats"
        //RID             rid;
        f = null;
        try {
            f = new Heapfile("reserves.in");
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 3, Rtypes, Rsizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int keyType=AttrType.attrInteger;
        try{
            btree_index = new BTreeFile("reserves.in"+".unclustered", keyType, 100, 0);
        }catch(Exception e){
            e.printStackTrace();
        }

        for (int i = 0; i < numreserves; i++) {
            try {
                t.setIntFld(1, ((Reserves) reserves.elementAt(i)).sid);
                t.setIntFld(2, ((Reserves) reserves.elementAt(i)).bid);
                t.setStrFld(3, ((Reserves) reserves.elementAt(i)).date);

            } catch (Exception e) {
                System.err.println("*** error in Tuple.setStrFld() ***");
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
                btree_index.insert(new IntegerKey(t.getIntFld(1)), rid);
//                System.out.println(t.noOfFlds() + " RID: " + rid.pageNo + "." + rid.slotNo);
            } catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                status = FAIL;
                e.printStackTrace();
            }
        }
        if (status != OK) {
            //bail out
            System.err.println("*** Error creating relation for reserves");
            Runtime.getRuntime().exit(1);
        }

    }

    public boolean runTests() {

        Disclaimer();
//        Query1();

//        Query2();
//        Query3();


//        Query4();
//        Query5();
//        Query6();
//        try{
//            Table t = new Table("boats.txt");
//            t.create_table("boats.txt", "boats.txt");
//            t.print_table();
//        }catch (Exception e){
//            e.printStackTrace();
//        }
        Query7();
        System.out.print("Finished joins testing" + "\n");


        return true;
    }

    private void Query7_CondExpr(CondExpr[] expr) {

        expr[0].next = null;
        expr[0].op = new AttrOperator(AttrOperator.aopEQ);

        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);

        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);

        expr[1] = null;
    }

    public void Query7() {
        try{
            System.out.print("**********************Query7 strating *********************\n");
            System.out.print("Query: Join sailors with reserves on id\n"
                    + "  SELECT   S.sname, S.sid\n"
                    + "  JOIN Sailors'S' 1  Reserves'R' 1 >= \n\n\n");

            CondExpr[] outFilter = new CondExpr[2];
            outFilter[0] = new CondExpr();
            outFilter[1] = new CondExpr();

            Query7_CondExpr(outFilter);
            Tuple t = new Tuple();
            t = null;

            AttrType[] Stypes = {
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrString),
                    new AttrType(AttrType.attrInteger),
            };

            short[] Ssizes = new short[1];
            Ssizes[0] = 30;
            AttrType[] Rtypes = {
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrString),
            };

            short[] Rsizes = new short[1];
            Rsizes[0] = 15;

            short[] JJsize = new short[1];
            JJsize[0] = 30;


            FldSpec[] proj1 = {
                    new FldSpec(new RelSpec(RelSpec.outer), 1),
                    new FldSpec(new RelSpec(RelSpec.outer), 2),
                    new FldSpec(new RelSpec(RelSpec.outer), 3),
                    new FldSpec(new RelSpec(RelSpec.innerRel), 1),
                    new FldSpec(new RelSpec(RelSpec.innerRel), 2),
                    new FldSpec(new RelSpec(RelSpec.innerRel), 3)
            };

            FldSpec[] Sprojection = {
                    new FldSpec(new RelSpec(RelSpec.outer), 1),
                    new FldSpec(new RelSpec(RelSpec.outer), 2),
                    new FldSpec(new RelSpec(RelSpec.outer), 3),
            };


            FileScan am = null;
            am = new FileScan("sailors.in", Stypes, Ssizes,
                    (short) 3, (short) 3,
                    Sprojection, null);

            IndexNestedLoopJoin inl = null;
            inl = new IndexNestedLoopJoin(Stypes, 3, Ssizes,
                    Rtypes, 3, Rsizes,
                    5,
                    am, "reserves.in",
                    outFilter, null, proj1, proj1.length);

            AttrType [] JJtype = {
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrString),
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrString)
            };
            while ((t = inl.get_next()) != null) {
                t.print(JJtype);
            }
            inl.close();
        }catch (Exception e) {
            System.err.println("*** Error preparing for nested_loop_join");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

    }

    private void Disclaimer() {
        System.out.print("\n\nAny resemblance of persons in this database to"
                + " people living or dead\nis purely coincidental. The contents of "
                + "this database do not reflect\nthe views of the University,"
                + " the Computer  Sciences Department or the\n"
                + "developers...\n\n");
    }
}


public class IndexJoinTest {
    public static void main(String argv[]) {
        boolean sortstatus;
        //SystemDefs global = new SystemDefs("bingjiedb", 100, 70, null);
        //JavabaseDB.openDB("/tmp/nwangdb", 5000);

        JoinsDriver jjoin = new JoinsDriver();

        sortstatus = jjoin.runTests();
        if (sortstatus != true) {
            System.out.println("Error ocurred during join tests");
        } else {
            System.out.println("join tests completed successfully");
        }
    }
}

