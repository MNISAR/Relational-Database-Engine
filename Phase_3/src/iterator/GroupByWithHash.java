package iterator;

import global.AggType;
import global.AttrType;

import hashindex.HashIndexWindowedScan;
import heap.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import btree.KeyDataEntry;
import btree.ScanIteratorException;

import static global.GlobalConst.MINIBASE_PAGESIZE;

public class GroupByWithHash extends Iterator{
    public static List<Tuple> _result;
    private static AttrType[] _attrType;
    private static int _len_in;
    private static boolean status = true;
    private static short[] _attr_sizes;
    private static AggType _agg_type;
    // number of tuples the queue can hold
    private int _n_pages;
    FldSpec[] _agg_list, _proj_list;
    int _n_out_flds;
    int fld;

    private HashIndexWindowedScan _hiwfs;
    private GroupByWithSort grpSort;

    public GroupByWithHash(
            AttrType[] in1, int len_in1, short[] t1_str_sizes,
            HashIndexWindowedScan am1,
            FldSpec group_by_attr,
            FldSpec[] agg_list,
            AggType agg_type,
            FldSpec[] proj_list,
            int n_out_flds,
            int n_pages
        )  {
        _attrType = in1;
        _len_in = len_in1;
        _attr_sizes = t1_str_sizes;
        _agg_type = agg_type;
        _n_pages = n_pages;
        _agg_list = agg_list;
        _proj_list = proj_list;

        _result = new ArrayList<>();

        _n_out_flds = n_out_flds;

        _outAttrType = new AttrType[proj_list.length];

        _outAttrType[0] = _attrType[group_by_attr.offset-1];

        for(int i=1; i<_n_out_flds; i++){
            _outAttrType[i] = new AttrType(AttrType.attrInteger);
        }

        _hiwfs = am1;
        fld = group_by_attr.offset;
    }


    public List<Tuple> get_next_aggr() throws Exception {
        _result = new ArrayList<>();
        Iterator it;
        if((it=_hiwfs.get_next())!=null){
            grpSort = new GroupByWithSort(_attrType,_len_in, _attr_sizes, it, new FldSpec(new RelSpec(RelSpec.outer), fld),
                    _agg_list, _agg_type, _proj_list, _n_out_flds, _n_pages);

            List<Tuple> iterator;

            while((iterator = grpSort.get_next_aggr()) != null){
                iterator.forEach((tuple) -> {
                    _result.add(tuple);
                });
            }
        }else{
            return null;
        }

        return _result;
    }

    @Override
    public Tuple get_next() throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException, SortException {
        _hiwfs.close();
        _hiwfs = null;
    }


	@Override
	public KeyDataEntry get_next_key_data() throws ScanIteratorException {
		// TODO Auto-generated method stub
		return null;
	}
}
