package iterator;

import btree.LeafData;
import bufmgr.*;
import global.*;
import heap.*;
import index.IndexException;

import java.io.IOException;
import java.util.Arrays;

import static tests.TestDriver.FAIL;
import static tests.TestDriver.OK;

public class SortFirstSky extends Iterator implements GlobalConst {

    private static int _n_pages;
    private static String _relationName;
    private static int[] _pref_list;
    private static int _pref_list_length;
    private Iterator am1;
    private Sort _sort;
    private static AttrType[] _in;
    private static short _len_in;
    private static short[] _str_sizes;
    private static Heapfile temp;
    private static FileScan _tscan; // for scanning the temp heap file
    boolean status = OK;
    private static short REC_LEN = 32;
    private static AttrType[] _attrType;
    private static short[] _attrSize;
    private static FldSpec[] _projlist;
    //private static LinkedHashSet<Tuple> _window;
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
        _attrSize = new short[_len_in];

        for(int i=0; i<_attrType.length; i++){
            _attrType[i] = new AttrType(AttrType.attrReal);
        }

        _relationName = relationName;
        _pref_list = pref_list;
        _pref_list_length = pref_list_length;
        _n_pages = n_pages; // (let one out for spare in case of temp heap)
        // _window = new LinkedHashSet<Tuple>(_n_pages);
        _window = new Tuple[(MINIBASE_PAGESIZE / _tuple_size) * (_n_pages/2)];
        //_window = new Tuple[1];
        // Sort "test1sortPref.in"
        /*
        try {
            _sort = new SortPref(_attrType, (short) _len_in, _attrSize, _fscan,  new TupleOrder(TupleOrder.Descending), _pref_list, _pref_list_length, 900);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

         */
        _sort = (Sort)am1;

        try {
            // temp heap file to store overflown skyline objects
            temp = new Heapfile("sortFirstSkyTemp.in");
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        /*
        System.out.println("----------   SORT FIRST SKY INIT VARS   -------------");
        System.out.println("Attributes length: "+_len_in);
        System.out.println("Relation name: "+_relationName);
        System.out.println("Preferences list: "+ Arrays.toString(_pref_list));
        System.out.println("Preferences list length: "+_pref_list_length);
        System.out.println("Size of each tuple: "+_tuple_size);
        System.out.println("Length of the buffer: "+_window.length);
        System.out.println("-----------------------------------------------------");
        */
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
            if(temp.getRecCnt() > 0)
                bnls.close();
                temp.deleteFile();
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
        SystemDefs.JavabaseBM.limit_memory_usage(false, _n_pages);

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
        /*
        while (t != null) {

            Tuple outer_tuple = new Tuple(t);
            boolean isDominatedBy = false;
            if(count == 0){
                window[count] = outer_tuple;
            }else {
                // compare outer_tuple with all the window objects here
                for (int i = 0; i < window.length; i++) {
                    if (window[i] != null) {
                        // if window[i] is not null
                        // check if window_tuple dominates outer_tuple
                        if (TupleUtils.Dominates(window[i], _attrType, outer_tuple, _attrType, _len_in, _str_sizes, _pref_list, _pref_list_length)) {
                            // If tuple in heap file is dominated by at least one in main memory - simply move to the next element
                            isDominatedBy = true;
                            System.out.println("Heap tuple");
                            outer_tuple.print(_in);
                            System.out.println("Dominated by ");
                            window[i].print(_in);
                            System.out.println(" ");
                            break;
                        }
                    }
                }
            }

            if(isDominatedBy == false) {
                System.out.println("Not dominated by any in the window");
                if (count < window.length) {
                    // move outer tuple to window
                    // If tuple in outer loop dominates the window object - replace the window object
                    window[count] = outer_tuple;
                } else {
                    // move outer tuple to heap
                    try {
                        rid = temp.insertRecord(outer_tuple.returnTupleByteArray());
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
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

         */

        while (t != null && count < window.length) {

            Tuple outer_tuple = new Tuple(t);
            boolean isDominatedBy = false;

            for (int i = 0; i < window.length; i++) {
                if (window[i] != null) {
                    // if window[i] is not null
                    // check if window_tuple dominates outer_tuple
                    if (TupleUtils.Dominates(window[i], _attrType, outer_tuple, _attrType, _len_in, _str_sizes, _pref_list, _pref_list_length)) {
                        // If tuple in heap file is dominated by at least one in main memory - simply move to the next element
                        isDominatedBy = true;
                        /*
                        System.out.println("Heap tuple");
                        outer_tuple.print(_in);
                        System.out.println("Dominated by ");
                        window[i].print(_in);
                        System.out.println(" ");

                         */
                        break;
                    }
                }
            }

            if(!isDominatedBy) {
                window[count++] = outer_tuple;
            }

            try {
                t = sort.get_next();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        while (t != null) {
            boolean isDominatedBy = false;
            Tuple htuple = new Tuple(t);

            for (int i = 0; i < window.length; i++) {
                if (window[i] != null) {
                    // if window[i] is not null
                    // check if window_tuple dominates outer_tuple
                    if (TupleUtils.Dominates(window[i], _attrType, htuple, _attrType, _len_in, _str_sizes, _pref_list, _pref_list_length)) {
                        // If tuple in heap file is dominated by at least one in main memory - simply move to the next element
                        isDominatedBy = true;
                        /*
                        System.out.println("Heap tuple");
                        htuple.print(_in);
                        System.out.println("Dominated by ");
                        window[i].print(_in);
                        System.out.println(" ");

                         */
                        break;
                    }
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

            try {
                t = sort.get_next();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }
        /*
        System.out.println("=====Window Skyline Tuples=====");

        for(int i=0; i<window.length; i++){
            if(window[i] != null)
                window[i].print(_in);
        }
        */
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
                "sortFirstSkyTemp.in",
                _pref_list,
                _pref_list_length,
                _n_pages/2
        );


        System.out.println("=====Temp heap Skyline Tuples=====");
        /*
        Tuple skl;
        try {
            skl = bnls.get_next();
            while (skl!=null) {
                skl.print(_in);
                skl = bnls.get_next();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        */
        //bnls.close();

        return;
    }

}