package iterator;

import bufmgr.PageNotReadException;
import global.*;
import heap.*;
import index.IndexException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import btree.KeyDataEntry;
import btree.ScanIteratorException;

import static global.GlobalConst.STRSIZE;


public class GroupByWithSort extends Iterator{
    public static List<Tuple> _result;
    private static Sort _sort;
    private static AttrType[] _attrType;
    private static FldSpec _group_by_attr;
    private static int _len_in;
    private static boolean status = true;
    private static short[] _attr_sizes;
    private static AggType _agg_type;
    private static FldSpec[] _agg_list;

    // heap file containing our data on which skyline is computed
    private Heapfile _skyline_grp_heap;

    // size of the tuples in the skyline/window/temporary_heap_file
    private int          candidate_tuple_size;

    // Value of the aggregation attribute in the last tuple
    private static float _lastPolled;

    // buffer pages allocation
    private int _n_pages;

    // get the next immediate tuple
    private Tuple _next;

    private static float _aggr_val;
    private static int _group_size;
    private static float _grp_result;
    private static Iterator _am;
    private static FldSpec[] _projlist;
    private static int _nOutFlds;

    public GroupByWithSort(
            AttrType[] in1, int len_in1, short[] t1_str_sizes,
            Iterator am1,
            FldSpec group_by_attr,
            FldSpec[] agg_list,
            AggType agg_type,
            FldSpec[] proj_list,
            int n_out_flds,
            int n_pages
    ){

        _attrType = in1;
        _len_in = len_in1;
        _attr_sizes = t1_str_sizes;
        _agg_type = agg_type;
        _n_pages = n_pages;
        _agg_list = agg_list;
        _group_by_attr = group_by_attr;
        _am = am1;
        _projlist = proj_list;
        _nOutFlds = n_out_flds;

        _outAttrType = new AttrType[proj_list.length];


        _outAttrType[0] = _attrType[_group_by_attr.offset-1];

        for(int i=1; i<_nOutFlds; i++){
            _outAttrType[i] = new AttrType(AttrType.attrInteger);
        }

        _lastPolled = 0.0f;

        _aggr_val = _agg_type.aggType == AggType.AVG ? 0.0f : _agg_type.aggType == AggType.MIN ? Float.MAX_VALUE : -Float.MIN_VALUE;
        _group_size = 0;
        _grp_result = 0.0f;

        _attr_sizes = new short[1];
        _attr_sizes[0] = STRSIZE;

        /* initialise tuple size */
        try {
            Tuple tuple_candidate = new Tuple();
            tuple_candidate.setHdr((short) _len_in, _attrType, this._attr_sizes);
            this.candidate_tuple_size = tuple_candidate.size();
        } catch (InvalidTypeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        _result = new ArrayList<>();

        try {
            _skyline_grp_heap = new Heapfile("skyline_group_by2.in");
        }
        catch (Exception e) {
            System.err.println("Could not open the skyline heapfile");
            e.printStackTrace();
        }

        try {
            _sort = new Sort(_attrType, (short) _len_in, _attr_sizes, _am, group_by_attr.offset, new TupleOrder(TupleOrder.Descending), 32, 3);
        }
        catch (Exception e) {
            status = false;
            e.printStackTrace();
        }

        if(!status) {
            System.exit(-1);
        }

    }

    public void skyline_Aggregation(String skyline_grp_heap, FldSpec[] pref_list, AttrType[] attrType, short[] attrSize, int buffer){
        BlockNestedLoopsSky blockNestedLoopsSky = null;
        System.out.println("Running skyline aggregation");
        int[] preference_list = new int[pref_list.length];
        for(int i=0; i<pref_list.length; i++){
            preference_list[i] = pref_list[i].offset;
        }


        try {
            blockNestedLoopsSky = new BlockNestedLoopsSky(attrType,
                    (short)attrType.length,
                    attrSize,
                    null,
                    skyline_grp_heap,
                    preference_list,
                    preference_list.length,
                    buffer);

            Tuple temp;

            Tuple result = new Tuple(this.candidate_tuple_size);

            try {
                result.setHdr((short) _nOutFlds, _outAttrType, _attr_sizes);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InvalidTypeException e) {
                e.printStackTrace();
            } catch (InvalidTupleSizeException e) {
                e.printStackTrace();
            }

            try {
                temp = blockNestedLoopsSky.get_next();
                while (temp!=null) {
                    Projection.Project(temp, _attrType, result, _projlist, _projlist.length);
                    _result.add(result);
                    temp = blockNestedLoopsSky.get_next();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        } catch (IOException | FileScanException | TupleUtilsException | InvalidRelation e) {
            e.printStackTrace();
        } finally {
            blockNestedLoopsSky.close();
        }
    }

    public List<Tuple> get_next_aggr() throws IOException, FieldNumberOutOfBoundException, UnknowAttrType, WrongPermat {
        _result = new ArrayList<>();

        /*
        TODO: Set projection attributes for skyline aggregation
        TODO: Define str sizes beforehand
        */



        Tuple result = new Tuple(this.candidate_tuple_size);

        try {
            result.setHdr((short) _nOutFlds, _outAttrType, _attr_sizes);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InvalidTypeException e) {
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        }

        Tuple t = null;

        try {
            t = _next == null ? _sort.get_next() : _next;

            if(t == null)
                return null;

            _lastPolled = t.getFloFld(_group_by_attr.offset);
        }
        catch (Exception e) {
            status = false;
            e.printStackTrace();
        }

        RID rid;

        while(t != null && t.getFloFld(_group_by_attr.offset) == _lastPolled){

            Projection.Project(t, _attrType, result, _projlist, _projlist.length);

            _group_size += 1;

            if (_agg_type.aggType == AggType.MIN) {
                _aggr_val = Math.min(_aggr_val, t.getFloFld(_agg_list[0].offset));
                _grp_result = _aggr_val;
            } else if (_agg_type.aggType == AggType.MAX) {
                _aggr_val = Math.max(_aggr_val, t.getFloFld(_agg_list[0].offset));
                _grp_result = _aggr_val;
            } else if (_agg_type.aggType == AggType.AVG) {
                _aggr_val += t.getFloFld(_agg_list[0].offset);
                _grp_result = _aggr_val / _group_size;
            } else if (_agg_type.aggType == AggType.SKYLINE) {
                // add to skyline group heap
                try {
                    rid = _skyline_grp_heap.insertRecord(t.returnTupleByteArray());
                }
                catch (Exception e) {
                    status = false;
                    e.printStackTrace();
                }
            }

            _lastPolled = t.getFloFld(_group_by_attr.offset);

            try {
                t = _sort.get_next();
                _next = t;
            }
            catch (Exception e) {
                status = false;
                e.printStackTrace();
            }
        }

        // Compute Skyline here
        if(_agg_type.aggType == AggType.SKYLINE) {
            skyline_Aggregation("skyline_group_by2.in", _agg_list, _attrType, _attr_sizes, _n_pages - 3);
            recreateSkyLineHeap();
            // Reset aggregation
            resetAggregation();
            return  _result;
        }

        // Construct result tuple here
        result.setFloFld(2,  _grp_result);
        _result.add(result);

        // Reset aggregation
        resetAggregation();

        return _result;
    }

    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, FieldNumberOutOfBoundException, FileAlreadyDeletedException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException {
        return null;
    }

    public void resetAggregation(){
        _aggr_val = _agg_type.aggType == AggType.AVG ? 0.0f : _agg_type.aggType == AggType.MIN ? Float.MAX_VALUE : -Float.MIN_VALUE;
        _group_size = 0;
        _grp_result = 0.0f;
    }

    public void recreateSkyLineHeap(){
        // delete heap file
        try {
            _skyline_grp_heap.deleteFile();
            _skyline_grp_heap = new Heapfile("skyline_group_by2.in");
        } catch (InvalidSlotNumberException e) {
            e.printStackTrace();
        } catch (FileAlreadyDeletedException e) {
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (HFException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException, SortException, JoinsException, IndexException {
        _am.close();
        _sort.close();
        _sort = null;
        _am = null;
        try {
            _skyline_grp_heap.deleteFile();
        } catch (InvalidSlotNumberException e) {
            e.printStackTrace();
        } catch (FileAlreadyDeletedException e) {
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        }
    }

	@Override
	public KeyDataEntry get_next_key_data() throws ScanIteratorException {
		// TODO Auto-generated method stub
		return null;
	}
}
