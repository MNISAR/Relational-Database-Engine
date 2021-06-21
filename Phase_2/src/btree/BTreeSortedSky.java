package btree;

import static tests.TestDriver.FAIL;
import static tests.TestDriver.OK;

import java.io.IOException;
import java.util.Arrays;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.ExtendedSystemDefs;
import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import btree.*;
import heap.FieldNumberOutOfBoundException;
import heap.FileAlreadyDeletedException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import iterator.FileScan;
import iterator.TupleUtils;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.PredEvalException;
import iterator.SortException;
import iterator.SortPref;
import iterator.TupleUtils;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;
import iterator.BlockNestedLoopsSky;


/**
 * BTreeSortedSky(
 * AttrType[] in1, 
 * int len_in1, 
 * short[] t1_str_sizes, 
 * int Iterator am1, 
 java.lang.String relationName,
 int[] pref_list,
 int[] pref_list_length,
 IndexFile index_file,
 int n_pages)
 * @author kunjpatel
 *
 */

public class BTreeSortedSky extends Iterator implements GlobalConst {

	private AttrType[] attrType;
	private int attr_len;
	private short[] t1_str_sizes;
	private Iterator am1;
	String relationName;
	private int[] pref_list;
	private int pref_list_length;
	private IndexFile index_file;
	private int n_pages;
	private int amt_of_mem;

	private int window_size;
	boolean status = OK;
	private static Tuple[] _window;
	private int counter;

	private Heapfile temp;
	private int temp_rcrd_count;

	private BlockNestedLoopsSky bnls;

	public BTreeSortedSky(AttrType[] attrType, int attr_len, short[] t1_str_sizes, int amt_of_mem, Iterator am1,
						  String relationName, int[] pref_list, int pref_list_length, IndexFile index_file,
						  int n_pages) throws Exception {

		this.counter = 0;
		this.relationName = relationName;
		this.index_file = (BTreeFile) index_file;
		this.attrType = attrType;
		this.attr_len = attr_len;
		this.t1_str_sizes = t1_str_sizes;
		this.am1 = am1;
		this.pref_list = pref_list;
		this.pref_list_length = pref_list_length;
		this.n_pages = n_pages;

		this.amt_of_mem = amt_of_mem;

	}



	public void computeSkylines() throws  Exception {
		Heapfile hf = new Heapfile("heap_" + "AAA");
		temp = new Heapfile("sortFirstSkyTemp.in");
		BTFileScan scan = ((BTreeFile) index_file).new_scan(null, null);
		KeyDataEntry entry;
		RID rid;

		Tuple t = getEmptyTuple();
		System.out.println("Number of pages "+n_pages);
		this.window_size = ((int)(MINIBASE_PAGESIZE/t.size()))*(n_pages/2);
		System.out.println("Tuple size "+t.size());
		System.out.println("SIZE: " + window_size);

		_window = new Tuple[window_size];
		System.out.println("Windows size in btree sorted sky: "+ _window.length);
		entry = scan.get_next();

		int count = 0;
		while (entry != null && count < _window.length) {
			Tuple temp = getEmptyTuple();
			rid = ((LeafData) entry.data).getData();
			temp.tupleCopy(hf.getRecord(rid));
			//temp.print(attrType);

			boolean isDominatedByWindow = checkDominationWithinWindowTuples(temp,count);

			if(!isDominatedByWindow) {
				_window[count++] = temp;
			}

			entry = scan.get_next();
		}
//	    System.out.println("In memory objects");
//        for(int i=0; i<_window.length; i++) {
//            if(_window[i] != null) {}
//                _window[i].print(attrType);
//        }


		while (entry != null) {
			boolean isDominatedBy = false;
			Tuple htuple = getEmptyTuple();

			rid = ((LeafData) entry.data).getData();
			htuple.tupleCopy(hf.getRecord(rid));

			for(int i=0; i<_window.length; i++){
				if (TupleUtils.DominatesForCombinedTree(_window[i] , attrType, htuple, attrType, (short) attr_len, t1_str_sizes, pref_list, pref_list_length)) {
					isDominatedBy = true;
					break;
				}
			}

			if(!isDominatedBy){
				try {
					rid = temp.insertRecord(htuple.returnTupleByteArray());

				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			entry = scan.get_next();
		}
		((BTreeFile) index_file).close();
		scan.DestroyBTreeFileScan();
		SystemDefs.JavabaseBM.flushAllPages();
		System.out.println("record count in temporary file: "+temp.getRecCnt());
		this.temp_rcrd_count = temp.getRecCnt();
		if( temp_rcrd_count == 0) return;

		bnls = new BlockNestedLoopsSky(
				attrType,
				attr_len,
				t1_str_sizes,
				am1,
				"sortFirstSkyTemp.in",
				pref_list,
				pref_list_length,
				n_pages/2
		);
		return;
	}

	private boolean checkDominationWithinWindowTuples(Tuple temp, int count) throws TupleUtilsException, UnknowAttrType, FieldNumberOutOfBoundException, IOException {
		if(count == 0) return false;

		for(int i = 0; i < count; i++) {
			if (TupleUtils.DominatesForCombinedTree(_window[i] , attrType, temp, attrType, (short) attr_len, t1_str_sizes, pref_list, pref_list_length)) {
				return true;
			}
		}

		return false;
	}


	private Tuple getEmptyTuple() throws InvalidTypeException, InvalidTupleSizeException, IOException {
		Tuple t = new Tuple();
		t.setHdr((short) attrType.length, attrType, t1_str_sizes);
		int size = t.size();
		t = new Tuple(size);
		t.setHdr((short) attrType.length, attrType, t1_str_sizes);
		return t;
	}



	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		// TODO Auto-generated method stub
		if ( this.counter < this.window_size ) {
			Tuple skl = _window[counter];
			counter++;

			if ( skl != null ) {
				return skl;
			}
		}
		if ( this.temp.getRecCnt() != 0 ) {
			Tuple skl = bnls.get_next();
			if ( skl != null ) {
				return skl;
			}
		}
		counter = this.window_size;
		return null;
	}



	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO Auto-generated method stub
		try {
			if ( this.temp.getRecCnt() != 0 ) {
				this.bnls.close();
			}
			temp.deleteFile();
		} catch (InvalidSlotNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileAlreadyDeletedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTupleSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFBufMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFDiskMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}