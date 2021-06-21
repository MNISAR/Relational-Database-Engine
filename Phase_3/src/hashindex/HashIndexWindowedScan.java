package hashindex;

import btree.IndexFile;
import btree.IndexFileScan;
import global.AttrType;
import global.IndexType;
import heap.*;
import index.IndexException;
import index.UnknownIndexTypeException;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.*;

import java.io.IOException;

public class HashIndexWindowedScan {
    public FldSpec[] perm_mat;
    private IndexFile indFile;
    private HIndex HindFile;
    private IndexFileScan indScan;
    private AttrType[] _types;
    private short[] _s_sizes;
    private CondExpr[] _selects;
    private int _noInFlds;
    private int _noOutFlds;
    private Heapfile f;
    private Tuple tuple1;
    private Tuple Jtuple;
    private int t1_size;
    private int _fldNum;
    private boolean index_only;
    private String main_heap_file, window_heap_file;
    private int n_windows, current_window;
    HashBucket bucket;
    Scan scan;


    public HashIndexWindowedScan(
            IndexType index,
            final String relName,
            final String indName,
            AttrType types[],
            short str_sizes[],
            int noInFlds,
            int noOutFlds,
            FldSpec outFlds[],
            CondExpr selects[],
            final int fldNum,
            final boolean indexOnly
        )
            throws IndexException,
            InvalidTypeException,
            InvalidTupleSizeException,
            UnknownIndexTypeException,
            IOException,
            Exception
    {
        _fldNum = fldNum;
        _noInFlds = noInFlds;
        _types = types;
        _s_sizes = str_sizes;

        AttrType[] Jtypes = new AttrType[noOutFlds];
        short[] ts_sizes;
        Jtuple = new Tuple();

        ts_sizes = TupleUtils.setup_op_tuple(Jtuple, Jtypes, types, noInFlds, str_sizes, outFlds, noOutFlds);

        _selects = selects;
        perm_mat = outFlds;
        _noOutFlds = noOutFlds;
        tuple1 = new Tuple();

        tuple1.setHdr((short) noInFlds, types, str_sizes);

        t1_size = tuple1.size();
        index_only = indexOnly;  // added by bingjie miao

        main_heap_file = relName;

        switch (index.indexType) {
            case IndexType.Hash:
                HindFile = new HIndex(indName);
                n_windows = HindFile.get_number_of_buckets();
                current_window = 0;
                break;
            default:
                throw new UnknownIndexTypeException("Only hash index is supported so far");
        }
    }

    public Iterator get_next()
            throws Exception {

        //System.out.println("Windows created "+n_windows);

        if (current_window > n_windows) {
            return null;
        }
        bucket = new HashBucket(HindFile.headerPage.get_NthBucketName(current_window));
        current_window += 1;
        window_heap_file = "";
        if(scan!=null){
            scan.closescan();
        }
        scan = bucket.heapfile.openScan();
        Iterator hifs = new HIndexedFileScan(main_heap_file,
                        scan,
                        _types,
                        _s_sizes,
                (short) _noInFlds,
                        _noOutFlds,
                        perm_mat);
        return hifs;
    }

    public void close(){
        scan.closescan();
        try {
			HindFile.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
