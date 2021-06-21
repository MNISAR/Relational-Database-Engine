package iterator;

import heap.*;
import index.*;
import iterator.Iterator;
//import tests.Reserves;
import global.*;
import bufmgr.*;
import btree.*;

import java.lang.*;
import java.io.*;
import java.util.*;

class TupleComparator implements Comparator<Tuple>{

	@Override
	public int compare(Tuple o1, Tuple o2) {
		try {
			if (o1.getFloFld( o1.noOfFlds() ) < o2.getFloFld( o2.noOfFlds() ) )
			  return 1;
			else if (o1.getFloFld( o1.noOfFlds() ) > o2.getFloFld( o2.noOfFlds() ))
			  return -1;
			else return 0;
		} catch (FieldNumberOutOfBoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}
}

public class TopK_HashJoin extends Iterator implements GlobalConst {
	
	AttrType[] in1; 
	int len_in1;
	short[] t1_str_sizes;
	FldSpec joinAttr1;
	FldSpec mergeAttr1;
	AttrType[] in2;
	int len_in2;
	short[] t2_str_sizes;
	FldSpec joinAttr2;
	FldSpec mergeAttr2;
	java.lang.String relationName1;
	java.lang.String relationName2;
	int k;
	int n_pages;
	
	PriorityQueue<Tuple> pq = null;
	HashJoin hj = null;
//	NestedLoopsJoins hj = null;
	
	public AttrType[] newAttrType = null;
	public short[] newAttrSize = null;
	
	public TopK_HashJoin(
			AttrType[] in1, int len_in1, short[] t1_str_sizes,
			FldSpec joinAttr1,
			FldSpec mergeAttr1,
			AttrType[] in2, int len_in2, short[] t2_str_sizes,
			FldSpec joinAttr2,
			FldSpec mergeAttr2,
			java.lang.String relationName1,
			java.lang.String relationName2,
			int k,
			int n_pages
			) throws JoinsException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		
		this.in1 = in1; 
		this.len_in1 = len_in1;
		this.t1_str_sizes = t1_str_sizes;
		this.joinAttr1 = joinAttr1;
		this.mergeAttr1 = mergeAttr1;
		this.in2 = in2;
		this.len_in2 = len_in2;
		this.t2_str_sizes = t2_str_sizes;
		this.joinAttr2 = joinAttr2;
		this.mergeAttr2 = mergeAttr2;
		this.relationName1 = relationName1;
		this.relationName2 = relationName2;
		this.k = k;
		this.n_pages = n_pages;
			    
	    Table table1 = SystemDefs.JavabaseDB.get_relation(this.relationName1);
		Table table2 = SystemDefs.JavabaseDB.get_relation(this.relationName2);
		
		AttrType[] table1_attr = table1.getTable_attr_type();
		int table1_len = table1.getTable_attr_type().length;
	    short[] table1_attr_size = table1.getTable_attr_size();
		
		AttrType[] table2_attr = table2.getTable_attr_type();
		int table2_len = table2.getTable_attr_type().length;
	    short[] table2_attr_size = table2.getTable_attr_size();

		
	    FldSpec[] projlist = new FldSpec[table1_len];
		RelSpec rel = new RelSpec(RelSpec.outer);
		
		for (int i=0; i<table1_len; i++ ) {
			projlist[i] = new FldSpec(rel, i+1);
		}
		
		FileScan am =  new FileScan(table1.getTable_heapfile(), 
				table1_attr,
				table1.getTable_attr_size(),
				   (short) table1.getTable_num_attr(),
				   (short) table1.getTable_num_attr(),
				   projlist, 
				   null);

	    FldSpec[] proj1 = new FldSpec[table1_len + table2_len];
	    	
	    int c = 0;
	    for(int i = 0; i < table1_len; i++) {
	    	proj1[c] =  new FldSpec(new RelSpec(RelSpec.outer), i+1);
	    	c++;
	    }
	    for(int i = 0; i < table2_len; i++) {
	    	proj1[c] =  new FldSpec(new RelSpec(RelSpec.innerRel), i+1);
	    	c++;
	    }
	    
	    CondExpr [] outFilter = new CondExpr[2];
	    outFilter[0] = new CondExpr();
	    outFilter[1] = new CondExpr();
    
	    outFilter[0].next  = null;
	    outFilter[0].op    = new AttrOperator(AttrOperator.aopEQ);
	    outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
	    outFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), joinAttr1.offset);
	    outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
	    outFilter[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),joinAttr2.offset);
	    outFilter[1] = null;
	    	     
	    hj = new HashJoin(
    		  table1_attr, table1_len, table1_attr_size,
    		  table2_attr, table2_len, table2_attr_size,
    		  100,
    		  am, table2.getTable_heapfile(),
    		  outFilter, null, proj1, table1_len + table2_len);
	    
	    
	    int newLength = table1_len + table2_len;
    	newAttrType = new AttrType[newLength];
        newAttrSize = new short[newLength];
        
        int pointer = 0;
        
        for(int i = 0; i < table1_len; i++) {
        	newAttrType[pointer] = table1_attr[i];
        	newAttrSize[pointer] = table1_attr_size[i];
        	pointer++;
        }
        for(int i = 0; i < table2_len; i++) {
        	if(i+1 == joinAttr2.offset) continue;
        	newAttrType[pointer] = table2_attr[i];
        	newAttrSize[pointer] = table2_attr_size[i];
        	pointer++;
        }
        
        newAttrType[pointer] = new AttrType(AttrType.attrReal); 
    	newAttrSize[pointer] = STRSIZE;
    		
	    Tuple t = hj.get_next();
  
	    pq = new 
                PriorityQueue<Tuple>(new TupleComparator());
	    
	    while(t != null) { 

	    	Tuple newTuple = new Tuple();
	    	newTuple.setHdr((short) newLength, newAttrType, newAttrSize);
	    	int newSize = newTuple.size();
	    	newTuple = new Tuple(newSize);
	    	newTuple.setHdr((short) newLength, newAttrType, newAttrSize);
	    	
	    	int curr = 1;
	    	for(int i = 1; i <= newLength; i++) {
	    		if(i == table1_len + joinAttr2.offset) {
	    			continue;
	    		}

	    		if(newAttrType[i-1].attrType == AttrType.attrString) {
	    			newTuple.setStrFld(curr, t.getStrFld(i));
	    		}
	    		else if (newAttrType[i-1].attrType == AttrType.attrInteger) {
	    			newTuple.setIntFld(curr, t.getIntFld(i));
	    		}
	    		else if(newAttrType[i-1].attrType == AttrType.attrReal) {
	    			newTuple.setFloFld(curr, t.getFloFld(i));
	    		}
	    		curr++;
	        }
	    	
	    	if(table1_attr[mergeAttr1.offset-1].attrType == AttrType.attrReal && table2_attr[mergeAttr2.offset-1].attrType == AttrType.attrReal) {
	    		newTuple.setFloFld(curr, (float) (t.getFloFld(mergeAttr1.offset) + 
		    			 t.getFloFld( table1_len + mergeAttr2.offset) / (float) 2.0)
		    	);
	    	}
	    	else if(table1_attr[mergeAttr1.offset-1].attrType == AttrType.attrInteger && table2_attr[mergeAttr2.offset-1].attrType == AttrType.attrInteger) {
	    		newTuple.setFloFld(curr, (float) ( t.getIntFld(mergeAttr1.offset) + 
		    			 t.getIntFld( table1_len + mergeAttr2.offset)) / (float) 2.0
		    	);
	    		
	    	}
	    	else if(table1_attr[mergeAttr1.offset-1].attrType == AttrType.attrInteger && table2_attr[mergeAttr2.offset-1].attrType == AttrType.attrReal) {
	    		newTuple.setFloFld(curr, (float) ( t.getIntFld(mergeAttr1.offset) + 
		    			 t.getFloFld( table1_len + mergeAttr2.offset)) / (float) 2.0
		    	);
	    	}
	    	else if(table1_attr[mergeAttr1.offset-1].attrType == AttrType.attrReal && table2_attr[mergeAttr2.offset-1].attrType == AttrType.attrInteger) {
	    		newTuple.setFloFld(curr, (float) ( t.getFloFld(mergeAttr1.offset) + 
		    			 t.getIntFld( table1_len + mergeAttr2.offset)) / (float) 2.0
		    	);
	    	}
	    	
	    	pq.add(newTuple);
	    	t = hj.get_next();
	    }
	}

	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {		
		
		if(k > 0) {
			Tuple t = pq.poll();
			k--;
			return t;
		}
		else {
			return null;
		}
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO Auto-generated method stub
		if ( hj!= null ) {
			hj.close();
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