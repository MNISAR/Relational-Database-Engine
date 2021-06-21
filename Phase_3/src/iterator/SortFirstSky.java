package iterator;

import btree.KeyDataEntry;
import btree.LeafData;
import btree.ScanIteratorException;
import bufmgr.*;
import global.*;
import heap.*;
import index.IndexException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static tests.TestDriver.FAIL;
import static tests.TestDriver.OK;

public class SortFirstSky extends Iterator implements GlobalConst {

    private static int _n_pages;
    private static String _relationName;
    private static int[] _pref_list;
    private static int _pref_list_length;
    private Sort _sort;
    private static AttrType[] _in;
    private static short _len_in;
    private static short[] _str_sizes;
    private static Heapfile temp;
    boolean status = OK;
    private static AttrType[] _attrType;
    private static Tuple[] _window;
    private static short _tuple_size;
    private int counter;
    private BlockNestedLoopsSky bnls;

    public SortFirstSky(AttrType[] in1, int len_in1, short[] t1_str_sizes,
                        Iterator am1, short tuple_size, java.lang.String
                                relationName, int[] pref_list, int pref_list_length,
                        int n_pages) throws Exception {


        _in = in1;
        _len_in = (short)len_in1;
        _str_sizes = t1_str_sizes;
        _tuple_size = tuple_size;
        this.counter = 0;

        _attrType = new AttrType[_len_in];

        for(int i=0; i<_attrType.length; i++){
            _attrType[i] = new AttrType(AttrType.attrReal);
        }

        _relationName = relationName;
        _pref_list = pref_list;
        _pref_list_length = pref_list_length;
        _n_pages = n_pages;
        _window = new Tuple[(MINIBASE_PAGESIZE / _tuple_size) * (_n_pages)];

        _sort = (Sort)am1;

        try {
            // temp heap file to store overflown skyline objects
            temp = new Heapfile(_relationName);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        if ( status == OK ) {
            computeSkylines(_sort, _window);

            // now check if temp_heap has records:
            // empty window; outer_heap <- temp_heap
            // delete outer loop
            // run computeSkyLines(outer_heap_updated, window)
        }

    }

    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
            InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
            LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        // TODO Auto-generated method stub
        if ( this.counter < _window.length ) {
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
        counter = _window.length;
        return null;
    }

    public boolean hasNext(){
        if(counter < _window.length)
            return _window[counter] != null;
        return false;
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {
        try {
            if(temp.getRecCnt() > 0) {
                bnls.close();
                temp.deleteFile();
            }
        } catch (InvalidSlotNumberException e) {
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (FileAlreadyDeletedException e) {
            e.printStackTrace();
        }

    }

    public void computeSkylines(Sort sort, Tuple[] window) throws Exception {

        /*
        SORT FIRST SKY(heap, window):

        1. Take the outer tuple through a sort iterator
        2. if window == empty; push the tuple to the window
        3. else compare outer tuple with the window objects
        4. if any window tuple dominates outer tuple -> move to the next tuple
        5. if any window tuple could not dominate outer tuple ->
            if count < window.length
                move outer tuple to window
            else
                move outer tuple to heap
        6. if temp_heap.getRecCnt() > 0:
            1. empty window
            2. treat temp_heap as outer_heap
            2.a delete heap
            3. run skyline using (temp_heap, window)

        */

        // create a tuple of appropriate size
        Tuple t = new Tuple();

        try {
            // check if there's atleast one tuple
            t = sort.get_next();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int count = 0;

        RID rid = null;

        while (t != null) {
            Tuple outer_tuple = new Tuple(t);
            boolean isDominatedBy = false;

            for (int i = 0; i < window.length; i++) {
                if (window[i] != null) {
                    // if window[i] is not null
                    // check if window_tuple dominates outer_tuple
                    if (TupleUtils.Dominates(window[i], _in, outer_tuple, _in, _len_in, _str_sizes, _pref_list, _pref_list_length)) {
                        // If tuple in heap file is dominated by at least one in main memory - simply move to the next element
                        isDominatedBy = true;
                        break;
                    }
                }
            }

            if(!isDominatedBy) {
                if(count < window.length)
                    window[count++] = outer_tuple;
                else{
                    try {
                        rid = temp.insertRecord(outer_tuple.returnTupleByteArray());
                    }
                    catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
            }

            try {
                t = sort.get_next();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        if( temp.getRecCnt() == 0)
            return;

        sort.close();

        // TODO: Run the bnl algorithm on the temp heap tuples
        SystemDefs.JavabaseBM.flushAllPages();

        bnls = new BlockNestedLoopsSky(
                _in,
                _in.length,
                null,
                null,
                _relationName,
                _pref_list,
                _pref_list_length,
                _n_pages
        );

        return;
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