package iterator;


import hashindex.HIndex;
import hashindex.HashIndexWindowedScan;
import hashindex.HashKey;
import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import iterator.*;
import btree.*;

import java.lang.*;
import java.util.List;
import java.io.*;

/**
 * This file contains an implementation of the index nested loops join
 * The algorithm is extremely simple:
 * <p>
 * foreach tuple r in R do
 * foreach s in index lookup in S do
 * if (ri == sj) then add (r, s) to the result.
 */

public class HashJoin extends Iterator {
    private AttrType _in1[], _in2[];
    private int in1_len, in2_len;
    private Iterator outer;
    private short t2_str_sizescopy[], t1_str_sizescopy[];
    private CondExpr OutputFilter[], rightFilter[];
    private CondExpr RightFilter[];
    private int n_buf_pgs;        // # of buffer pages available.
    private boolean done,         // Is the join complete
            get_from_outer;                 // if TRUE, a tuple is got from outer
    private Tuple outer_tuple, inner_tuple;
    private Tuple Jtuple;           // Joined tuple
    private FldSpec perm_mat[];
    private int nOutFlds;
    private Heapfile hf;
    private Scan inner;

    private Operand temp_op;
    private int fld1, fld2;  // index filed for inner relation
    private boolean index_found;
    private String inner_hash_index_name="hash-join-inner-index.unclustered",
                    outer_hash_index_name= "hash-join-outer-index.unclustered",
                    outer_temp_heap_name = "hash-join-outer-heap.in",
                    temp_temp_inner_heap_name = "hash-join-inner-temp-temp-heap.in";
    private int inner_relation_attrType, outer_relation_attrType;
    private int key_size=10, target_utilization=50;
    private int n_win1, n_win2, n_current;
    private HIndex outer_h, inner_h;
    private HashIndexWindowedScan outer_hiws, inner_hiws;
    private FldSpec[] outer_projection, inner_projection;
    Iterator it=null, it_outer=null,it_inner=null;
    AttrType[] Jtypes;
    short[] t_size;
    private String innertablename;
    private String outertablename;
    private Table inner_table, outer_table;

    /**
     * constructor
     * Initialize the two relations which are joined, including relation type,
     *
     * @param in1          Array containing field types of R.
     * @param len_in1      # of columns in R.
     * @param t1_str_sizes shows the length of the string fields.
     * @param in2          Array containing field types of S
     * @param len_in2      # of columns in S
     * @param t2_str_sizes shows the length of the string fields.
     * @param amt_of_mem   IN PAGES
     * @param am1          access method for left i/p to join
     * @param relationName access hfapfile for right i/p to join
     * @param outFilter    select expressions
     * @param rightFilter  reference to filter applied on right i/p
     * @param proj_list    shows what input fields go where in the output tuple
     * @param n_out_flds   number of outer relation fileds
     * @throws IOException         some I/O fault
     * @throws NestedLoopException exception from this class
     */
    public HashJoin(AttrType in1[],
                               int len_in1,
                               short t1_str_sizes[],
                               AttrType in2[],
                               int len_in2,
                               short t2_str_sizes[],
                               int amt_of_mem,
                               Iterator am1,
                               String relationName,
                               CondExpr outFilter[],
                               CondExpr rightFilter[],
                               FldSpec proj_list[],
                               int n_out_flds
    ) throws Exception {

        _in1 = new AttrType[in1.length];
        _in2 = new AttrType[in2.length];
        System.arraycopy(in1, 0, _in1, 0, in1.length);
        System.arraycopy(in2, 0, _in2, 0, in2.length);
        in1_len = len_in1;
        in2_len = len_in2;

        outer = am1;
        t2_str_sizescopy = t2_str_sizes;
        t1_str_sizescopy = t1_str_sizes;
        inner_tuple = new Tuple();
        Jtuple = new Tuple();
        AttrType[] Jtypes = new AttrType[n_out_flds];
        OutputFilter = outFilter;
        RightFilter = rightFilter;

        String[] tokens = relationName.split("\\.");
		this.innertablename = tokens[0];
		this.outertablename = am1.tablename;
		inner_table = SystemDefs.JavabaseDB.get_relation(innertablename);
		outer_table = SystemDefs.JavabaseDB.get_relation(outertablename);
		
        n_buf_pgs = amt_of_mem;
        inner = null;
        done = false;
        get_from_outer = true;

        perm_mat = proj_list;
        nOutFlds = n_out_flds;

        outer_projection = new FldSpec[_in1.length];
        inner_projection = new FldSpec[_in2.length];
        int j=0,k=0;
        for(int i=0;i<_in1.length;i++) {
            outer_projection[j] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
            j++;
        }
        for(int i=0;i<_in2.length;i++) {
            inner_projection[k] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
            k++;
        }

        try {
            t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
                    in1, len_in1, in2, len_in2,
                    t1_str_sizes, t2_str_sizes,
                    proj_list, nOutFlds);
        }catch (TupleUtilsException e){
            throw new NestedLoopException(e,"TupleUtilsException is caught by NestedLoopsJoins.java");
        }

        fld1 = OutputFilter[0].operand1.symbol.offset; // for outer relation
        fld2 = OutputFilter[0].operand2.symbol.offset; // for inner relation

        index_found = false;
        // check if index exists on the inner relation
        // TODO: ??

        if(inner_table!=null && inner_table.getHash_unclustered_attr()[fld2]){
            // index exists
            index_found = true;
            inner_hash_index_name = "hash-join-inner-index.unclustered";
        }

        outer_relation_attrType = _in2[fld1-1].attrType;
        inner_relation_attrType = _in1[fld2-1].attrType;

        HashKey key=null;
        RID rid = new RID();
        if ( inner_table == null ) {
	        inner_h = new HIndex(inner_hash_index_name, inner_relation_attrType, key_size,target_utilization);
	        Tuple tup;
	        if(!index_found){
	            // will have to create hash index on inner.
	            // creating hash index on inner relation.
	            Scan s = (new Heapfile(relationName)).openScan();
	            while((tup=s.getNext(rid))!=null){
	                tup.setHdr((short)in2_len, _in2, t2_str_sizes);
	                switch(inner_relation_attrType) {
	                    case AttrType.attrInteger:
	                        key = new HashKey(tup.getIntFld(fld2));
	                        break;
	                    case AttrType.attrReal:
	                        key = new HashKey(tup.getFloFld(fld2));
	                        break;
	                    case AttrType.attrString:
	                        key = new HashKey(tup.getStrFld(fld2));
	                        break;
	                    default:
	                        System.out.println("Not supposted type for inner relation index.");
	                }
	                inner_h.insert(key, rid);
	            }
	            s.closescan();
	            System.out.println("Hindex on inner relation created.");
	        }
        }
        else {
        	if ( inner_table.unclustered_index_exist(fld2, "hash") ) {
        		//unclustered hash already exist
        	}
        	else {
        		//create and unclustered hash
        		inner_table.create_unclustered_index(fld2, "hash");
        	}
        	inner_hash_index_name = inner_table.get_unclustered_index_filename(fld2, "hash");
        	outer_temp_heap_name = outer_table.getTable_heapfile();
        	inner_h = new HIndex(inner_hash_index_name);
        }
        n_win1 = inner_h.get_number_of_buckets();
        inner_h.close();
        
        // create heap file for outer relation and create hash index on it.
        if ( outer_table == null ) {
        	Heapfile temp_hf = new Heapfile(outer_temp_heap_name);
	        outer_h = new HIndex(outer_hash_index_name, outer_relation_attrType, key_size, target_utilization);
	        Tuple tup1 = null;
	        while((tup1= am1.get_next())!=null){
	            tup1.setHdr((short)len_in1, _in1, t1_str_sizes);
	            rid = temp_hf.insertRecord(tup1.getTupleByteArray());
	            switch(outer_relation_attrType) {
	                case AttrType.attrInteger:
	                    key = new HashKey(tup1.getIntFld(fld1));
	                    break;
	                case AttrType.attrReal:
	                    key = new HashKey(tup1.getFloFld(fld1));
	                    break;
	                case AttrType.attrString:
	                    key = new HashKey(tup1.getStrFld(fld1));
	                    break;
	                default:
	                    System.out.println("Not supposted type for inner relation index.");
	            }
	            outer_h.insert(key, rid);
	        }
        }
        else {
        	if ( outer_table.unclustered_index_exist(fld1, "hash") ) {
        		//unclustered hash already exist
        	}
        	else {
        		System.out.println("A clustered hash index does not exist on the table\nCreating one....");
        		//create and unclustered hash
        		outer_table.create_unclustered_index(fld1, "hash");
        		
        	}
        	outer_hash_index_name = outer_table.get_unclustered_index_filename(fld1, "hash");
    		outer_temp_heap_name = outer_table.getTable_heapfile();
        	outer_h = new HIndex(outer_hash_index_name);
        }
        n_win2 = outer_h.get_number_of_buckets();
        outer_h.close();
        
        n_current = 0;
        outer_hiws = new HashIndexWindowedScan(new IndexType(IndexType.Hash), outer_temp_heap_name, outer_hash_index_name,
                                                _in1, t1_str_sizes, in1_len, outer_projection.length, outer_projection,
                                        null, fld1, false);
        inner_hiws = new HashIndexWindowedScan(new IndexType(IndexType.Hash), relationName, inner_hash_index_name,
                                                _in2, t2_str_sizes, in2_len, inner_projection.length, inner_projection,
                                        null, fld2, false);
    }

    /**
     * @return The joined tuple is returned
     * @throws IOException               I/O errors
     * @throws JoinsException            some join exception
     * @throws IndexException            exception from super class
     * @throws InvalidTupleSizeException invalid tuple size
     * @throws InvalidTypeException      tuple type not valid
     * @throws PageNotReadException      exception from lower layer
     * @throws TupleUtilsException       exception from using tuple utilities
     * @throws PredEvalException         exception from PredEval class
     * @throws SortException             sort exception
     * @throws LowMemException           memory error
     * @throws UnknowAttrType            attribute type unknown
     * @throws UnknownKeyTypeException   key type unknown
     * @throws Exception                 other exceptions
     */
    public Tuple get_next()
            throws IOException,
            JoinsException,
            IndexException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            TupleUtilsException,
            PredEvalException,
            SortException,
            LowMemException,
            UnknowAttrType,
            UnknownKeyTypeException,
            Exception
    {
        Tuple t=null;
        if(it==null){
            it_outer = outer_hiws.get_next();
            it_inner = inner_hiws.get_next();
            if(it_inner==null || it_outer==null){
                return null;
            }
            else{
                create_heap_for_inner_iterator();
                /*System.out.println("\n\nOuter file:");
                Tuple temp;
                while((temp=it_outer.get_next())!=null){
                    temp.setHdr((short)in1_len, _in1, t1_str_sizescopy);
                    temp.printTuple(_in1);
                }
                System.out.println("\n\nInner File:");
                Scan temps = (new Heapfile(temp_temp_inner_heap_name)).openScan();
                RID rid=new RID();
                while((temp=temps.getNext(rid))!=null){
                    temp.setHdr((short)in2_len, _in2, t2_str_sizescopy);
                    temp.printTuple(_in2);
                }*/
                it = new NestedLoopsJoins(_in1, in1_len, t1_str_sizescopy,
                                            _in2, in2_len, t2_str_sizescopy,
                                            n_buf_pgs, it_outer, temp_temp_inner_heap_name,
                                            OutputFilter, RightFilter, perm_mat, nOutFlds);
            }
        }
        t = it.get_next();
        while(t==null){
            it_outer = outer_hiws.get_next();
            it_inner = inner_hiws.get_next();
            if(it_inner==null || it_outer==null){
                return null;
            }
            else{
                create_heap_for_inner_iterator();
                /*System.out.println("\n\nOuter file:");
                Tuple temp;
                while((temp=it_outer.get_next())!=null){
                    temp.setHdr((short)in1_len, _in1, t1_str_sizescopy);
                    temp.printTuple(_in1);
                }
                System.out.println("\n\nInner File:");
                Scan temps = (new Heapfile(temp_temp_inner_heap_name)).openScan();
                RID rid=new RID();
                while((temp=temps.getNext(rid))!=null){
                    temp.setHdr((short)in2_len, _in2, t2_str_sizescopy);
                    temp.printTuple(_in2);
                }*/
                it = new NestedLoopsJoins(_in1, in1_len, t1_str_sizescopy,
                        _in2, in2_len, t2_str_sizescopy,
                        n_buf_pgs, it_outer, temp_temp_inner_heap_name,
                        OutputFilter, RightFilter, perm_mat, nOutFlds);
            }
            if((t = it.get_next())!=null){
                return t;
            }
        }

        return t;

    }

    private void create_heap_for_inner_iterator() throws Exception{
        Heapfile temp_temp_heap = new Heapfile(temp_temp_inner_heap_name);
        temp_temp_heap.deleteFile();
        temp_temp_heap = new Heapfile(temp_temp_inner_heap_name);
        Tuple t;
        while((t=it_inner.get_next())!=null){
            temp_temp_heap.insertRecord(t.getTupleByteArray());
        }
    }

    /*
     * uses the filter on Output table to get the key for index lookup.
     */
    private void set_keys(Tuple outer) {
        CondExpr temp_ptr = OutputFilter[0];
        try {
            switch (temp_ptr.op.attrOperator) {
                case AttrOperator.aopEQ:
                    break;
                case AttrOperator.aopNE:
                    System.out.println("{NE} This operator is not supported.");
                    break;
                case AttrOperator.aopLT:
                    break;
                case AttrOperator.aopLE:
                    break;
                case AttrOperator.aopGT:
                    AttrType temp_ = temp_ptr.type1;
                    temp_ptr.type1 = temp_ptr.type2;
                    temp_ptr.type2 = temp_;

                    temp_op = temp_ptr.operand1;
                    temp_ptr.operand1 = temp_ptr.operand2;
                    temp_ptr.operand2 = temp_op;

                    temp_ptr.op.attrOperator = AttrOperator.aopLT;
                    break;
                case AttrOperator.aopGE:
                    AttrType temp__ = temp_ptr.type1;
                    temp_ptr.type1 = temp_ptr.type2;
                    temp_ptr.type2 = temp__;

                    temp_op = temp_ptr.operand1;
                    temp_ptr.operand1 = temp_ptr.operand2;
                    temp_ptr.operand2 = temp_op;

                    temp_ptr.op.attrOperator = AttrOperator.aopLE;
                    break;
                case AttrOperator.aopNOT:
                    System.out.println("{NOT} This operator is not supported.");
                    break;
                default:
                    break;
            }
            switch (temp_ptr.type1.attrType) {
                case AttrType.attrInteger:
                    break;
                case AttrType.attrReal:
                    break;
                case AttrType.attrString:
                    break;
                case AttrType.attrSymbol:
                    int fld1 = temp_ptr.operand1.symbol.offset;
                    switch (_in1[fld1 - 1].attrType) {
                        case AttrType.attrInteger:
                            break;
                        case AttrType.attrReal:
                            break;
                        case AttrType.attrString:
                            break;
                        default:
                    }
                    break;
            }
            switch (temp_ptr.type2.attrType) {
                case AttrType.attrInteger:
                    break;
                case AttrType.attrReal:
                    break;
                case AttrType.attrString:
                    break;
                case AttrType.attrSymbol:
                    int fld2 = temp_ptr.operand2.symbol.offset;
                    switch (_in1[fld2 - 1].attrType) {
                        case AttrType.attrInteger:
                            break;
                        case AttrType.attrReal:
                            break;
                        case AttrType.attrString:
                            break;
                        default:
                    }
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * implement the abstract method close() from super class Iterator
     * to finish cleaning up
     *
     * @throws IOException    I/O error from lower layers
     * @throws JoinsException join error from lower layers
     * @throws IndexException index access error
     */
    public void close() throws JoinsException, IOException, IndexException {
        if (!closeFlag) {
            try {
                outer.close();
                if(index_found){
//                    btreefile.close();
                }
                outer_hiws.close();
                inner_hiws.close();
            } catch (Exception e) {
                throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }
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