package hashindex;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.RID;
import heap.*;
import index.IndexException;
import iterator.*;

import java.io.IOException;
import java.util.List;

import btree.KeyDataEntry;
import btree.ScanIteratorException;

/*
  Given the name of heap file for a window,
  we iterate ofer the window heap file and return the
  tuples from main heap file.
 */
public class HIndexedFileScan extends Iterator {

    private String main_heap_file;
//    private String window_heap_file;
    private AttrType _in1[];
    private short s1_sizes[];
    private short _in1_len;
    private     int n_out_flds;
    private FldSpec[] proj_lis;
    Tuple Jtuple;
    Heapfile main_heap, window_heap;
    Scan main_scan, window_scan;
    RID rid;
    Tuple tup;
    int count = 0;
    HashEntry scannedHashEntry;


    public HIndexedFileScan(String main_heap_file,
                            Scan window_scan,
                            AttrType in1[],
                            short s1_sizes[],
                            short len_in1,
                            int n_out_flds,
                            FldSpec[] proj_list
    )
            throws Exception
    {
        this.main_heap_file = new String(main_heap_file);
//        this.window_heap_file = new String(window_heap_file);
        this._in1 = in1;
        this._in1_len = len_in1;
        this.n_out_flds = n_out_flds;
        this.s1_sizes = s1_sizes;
        this.proj_lis = proj_list;
        main_heap = new Heapfile(main_heap_file);
//        window_heap = new Heapfile(window_heap_file);
//        main_scan = main_heap.openScan();
        this.window_scan = window_scan;
    }

    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        rid = new RID();
        tup = window_scan.getNext(rid);
        if (tup == null) {
            return null;
        }
        scannedHashEntry = new HashEntry(tup.returnTupleByteArray(), 0);

//        System.out.println("  <"+scannedHashEntry.key.value+","+scannedHashEntry.rid+">");

//        main_scan.position(scannedHashEntry.rid);
//        Jtuple = main_scan.getNext(rid);
        Jtuple = main_heap.getRecord(scannedHashEntry.rid);
        Jtuple.setHdr(_in1_len, _in1, s1_sizes);
        return Jtuple;
    }

    public void close() throws IOException, JoinsException, SortException, IndexException {
        main_scan.closescan();
        main_heap = null;
    }

	@Override
	public List<Tuple> get_next_aggr() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KeyDataEntry get_next_key_data() throws ScanIteratorException {
		// TODO Auto-generated method stub
		return null;
	}
}
