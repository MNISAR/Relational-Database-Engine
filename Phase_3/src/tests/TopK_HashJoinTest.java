package tests;

import java.io.IOException;

import bufmgr.*;
import diskmgr.*;
import global.*;
import heap.*;
import index.*;
import iterator.*;
import java.util.*;
import btree.*;

class TopK_HashJoinDriver extends TestDriver
implements GlobalConst {
	
	public TopK_HashJoinDriver() {
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
        
        AttrType[] attrType = new AttrType[2];
        attrType[0] = new AttrType(AttrType.attrReal);
        attrType[1] = new AttrType(AttrType.attrReal);
        
        short[] attrSize = null;
        
        
        FldSpec joinAttr = new FldSpec(new RelSpec(RelSpec.outer), 1);
        FldSpec mergeAttr = new FldSpec(new RelSpec(RelSpec.outer), 2);
 
        try {
			TopK_HashJoin tkhj = new TopK_HashJoin(
					attrType, attrType.length, attrSize,
					joinAttr,
					mergeAttr,
					attrType, attrType.length, attrSize,
					joinAttr,
					mergeAttr,
					"heap_AAA1",
					"heap_AAA2",
					2,
					100
					);
		}  catch (Exception e) {
			e.printStackTrace();
		}
        
        boolean status = OK;
		return status;
    }
}

public class TopK_HashJoinTest  {
	
	public static void main(String[] argv) throws HFDiskMgrException, HFException, HFBufMgrException, IOException {
		TopK_HashJoinDriver tknj = new TopK_HashJoinDriver();
		tknj.runTests();
	}

}