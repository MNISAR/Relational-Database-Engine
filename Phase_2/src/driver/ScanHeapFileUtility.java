package driver;

import global.AttrType;
import global.RID;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;

public class ScanHeapFileUtility {
	
	/**
	 * utility function to scan and print the heapfile
	 */
    public static void scanHeap(Heapfile f, AttrType[] attrType, short[] attrSize) throws Exception {
    	Scan scan = new Scan(f);
    	RID trid = new RID();
    	Tuple t = new Tuple();
    	t.setHdr((short) attrType.length, attrType, attrSize);
    	int size = t.size();
    	t = new Tuple(size);
    	t.setHdr((short) attrType.length, attrType, attrSize);

    	Tuple temp = scan.getNext(trid);
    	while (temp != null) {
    		t.tupleCopy(temp);
    		t.print(attrType);
    		temp = scan.getNext(trid);
    	}
    	scan.closescan();

    }

}
