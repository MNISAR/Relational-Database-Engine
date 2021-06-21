package iterator;

import heap.*;
import index.IndexException;
import index.IndexScan;
import iterator.Iterator;
import global.*;
import bufmgr.*;
import clustered_btree.ClusteredBTreeFile;

import java.lang.*;
import java.io.*;
import java.util.*;

import btree.*;

class TupleComparatorNRA implements Comparator<Tuple>{

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

public class TopK_NRAJoin extends Iterator implements GlobalConst {
	
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
	IndexScan iscan1 = null;
	IndexScan iscan2 = null;
	
	boolean firstEntry = true;
	
    HashMap<String, NRABounds> map = new HashMap<>();
    PriorityQueue<Tuple> pq = null;
    
    IndexScan scan = null;
    
    public AttrType joinAttrType[];
    public short[] joinAttrSize;
    public boolean index_exist = true;
    
	public TopK_NRAJoin(
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
	) {
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
		
		pq = new PriorityQueue<Tuple>(new TupleComparatorNRA());
	}
	
	
	public void calculateTopKJoins() throws Exception {
		
		Table table1 = SystemDefs.JavabaseDB.get_relation(this.relationName1);
		Table table2 = SystemDefs.JavabaseDB.get_relation(this.relationName2);

		if(table1.clustered_index_exist(this.mergeAttr1.offset, this.relationName1)) {
			System.err.println("No Clustered index found on table "+ this.relationName1);
			index_exist = false;
			return;
		}
		if(table2.clustered_index_exist(this.mergeAttr1.offset, this.relationName2)) {
			System.err.println("No Clustered index found on table "+ this.relationName2);
			index_exist = false;
			return;
		}
		
		AttrType[] table1_attr = table1.getTable_attr_type();
		int table1_len = table1.getTable_attr_type().length;
	    short[] table1_attr_size = table1.getTable_attr_size();
		
		AttrType[] table2_attr = table2.getTable_attr_type();
		int table2_len = table2.getTable_attr_type().length;
	    short[] table2_attr_size = table2.getTable_attr_size();
	    
	    FldSpec[] projlist1 = new FldSpec[table1_len];
		RelSpec rel1 = new RelSpec(RelSpec.outer);
		
	    for (int i=0; i<table1_len; i++ ) {
			projlist1[i] = new FldSpec(rel1, i+1);
		}
	    
	    FldSpec[] projlist2 = new FldSpec[table2_len];
		RelSpec rel2 = new RelSpec(RelSpec.outer);
		
	    for (int i=0; i<table2_len; i++ ) {
			projlist2[i] = new FldSpec(rel2, i+1);
		}
		
		iscan1 = new IndexScan(new IndexType(IndexType.Cl_B_Index_DESC), 
					  this.relationName1, 
					  table1.get_clustered_index_filename(this.mergeAttr1.offset, "btree"), 
					  table1_attr, 
					  table1_attr_size, 
					  table1.getTable_num_attr(), 
					  table1.getTable_num_attr(), 
					  projlist1, 
					  null,
					  table1.getTable_num_attr(), 
					  false);
		
		iscan2 = new IndexScan(new IndexType(IndexType.Cl_B_Index_DESC), 
		  this.relationName2, 
		  table2.get_clustered_index_filename(this.mergeAttr2.offset, "btree"), 
		  table2_attr, 
		  table2_attr_size, 
		  table2.getTable_num_attr(), 
		  table2.getTable_num_attr(), 
		  projlist2, 
		  null,
		  table2.getTable_num_attr(), 
		  false);
		
		Tuple temp1 = iscan1.get_next();
		Tuple temp2 = iscan2.get_next();
 
        int objectsSeen = 0;
    	float currDepthScore = 0.0f;
    	float minLowerBound = 0.0f;
    	
    	int depth = 0;
    	
    	int newLength = table1_len + table2_len;
		joinAttrType = new AttrType[newLength];
		joinAttrSize = new short[newLength];

		int count = 0;
		for(int i = 0; i < table1_len; i++) {
			joinAttrType[count] = table1_attr[i];
			joinAttrSize[count] = table1_attr_size[i];
			count++;
		}
		for(int i = 0; i < table2_len; i++) {
			if(i+1 == joinAttr2.offset) continue;
			joinAttrType[count] = table2_attr[i];
			joinAttrSize[count] = table2_attr_size[i];
			count++;
		}
		
		joinAttrType[count] = new AttrType(AttrType.attrReal);
		joinAttrSize[count] = STRSIZE;
		
		boolean relation1Ended = false;
		boolean relation2Ended = false;
    	
    	while(true) {
    		depth += 1;
    		
    		if(relation1Ended && relation2Ended) break;
    		
    		if(temp1 == null) {
    			relation1Ended = true;
    		}
    		if(temp2 == null) {
    			relation2Ended = true;
    		}
    		
    		String joinKey1 = "";
    		String mapKey1 = "";
    		
    		String joinKey2 = "";
    		String mapKey2 = "";
    		
    		if(!relation1Ended) {
    			// For relation 1
        		if(table1_attr[joinAttr1.offset-1].attrType == AttrType.attrInteger) {
        			joinKey1 = "" + temp1.getIntFld(joinAttr1.offset) + "-2";
        			mapKey1 = temp1.getIntFld(joinAttr1.offset) + "-1";
        		}
        		else if (table1_attr[joinAttr1.offset-1].attrType == AttrType.attrReal) {
        			joinKey1 = "" + temp1.getFloFld(joinAttr1.offset) + "-2";
        			mapKey1 = temp1.getFloFld(joinAttr1.offset) + "-1";
        		}
        		else if (table1_attr[joinAttr1.offset-1].attrType == AttrType.attrString) {
        			joinKey1 = "" + temp1.getStrFld(joinAttr1.offset) + "-2";
        			mapKey1 = temp1.getStrFld(joinAttr1.offset) + "-1";
        		}
    		}
    		
    		if(!relation2Ended) {
    		// For relation 2
	    		if(table2_attr[joinAttr2.offset-1].attrType == AttrType.attrInteger) {
	    			joinKey2 = "" + temp2.getIntFld(joinAttr2.offset) + "-1";
	    			mapKey2 = temp2.getIntFld(joinAttr2.offset) + "-2";
	    		}
	    		else if (table2_attr[joinAttr2.offset-1].attrType == AttrType.attrReal) {
	    			joinKey2 = "" + temp2.getFloFld(joinAttr2.offset) + "-1";
	    			mapKey2 = temp2.getFloFld(joinAttr2.offset) + "-2";
	    		}
	    		else if (table2_attr[joinAttr2.offset-1].attrType == AttrType.attrString) {
	    			joinKey2 = "" + temp2.getStrFld(joinAttr2.offset) + "-1";
	    			mapKey2 = temp2.getStrFld(joinAttr2.offset) + "-2";
	    		}
    		}
    		
    		float merge1 = 0.0f;
    		float merge2 = 0.0f;
    		
    		if(!relation1Ended) {
	    		if(table1_attr[mergeAttr1.offset-1].attrType == AttrType.attrInteger) {
	    			merge1 = (float) temp1.getIntFld(mergeAttr1.offset);
	    		}
	    		else if(table1_attr[mergeAttr1.offset-1].attrType == AttrType.attrReal) {
	    			merge1 = temp1.getFloFld(mergeAttr1.offset);
	    		}
    		}
    		
    		
    		if(!relation2Ended) {
	    		if(table2_attr[mergeAttr2.offset-1].attrType == AttrType.attrInteger) {
	    			merge2 = (float) temp2.getIntFld(mergeAttr2.offset);
	    		}
	    		else if(table2_attr[mergeAttr2.offset-1].attrType == AttrType.attrReal) {
	    			merge2 = temp2.getFloFld(mergeAttr2.offset);
	    		}
    		}
    		
    		currDepthScore = merge1 + merge2;
    		
//    		System.out.println("***********************************");
//    		System.out.println("objectsSeen: " + objectsSeen);
//    		System.out.println("currDepthScore: " + currDepthScore);
//    		System.out.println("minLowerBound: " + minLowerBound);
//        	System.out.println("***********************************");
        	
    		if(objectsSeen >= k && currDepthScore < minLowerBound) break;
    		
    		if(!relation1Ended) {
	     		if(map.containsKey(joinKey1)) {
	    			NRABounds temp = map.get(joinKey1);
//	     			temp.t1 = temp1;
	    				
					Tuple mergedTuple = new Tuple();
					mergedTuple.setHdr((short) newLength, joinAttrType, joinAttrSize);
			    	int newSize = mergedTuple.size();
			    	mergedTuple = new Tuple(newSize);
			    	mergedTuple.setHdr((short) newLength, joinAttrType, joinAttrSize);
			    	
			    	count = 1;
					for(int i = 0; i < table1_len; i++) {
						if(table1_attr[i].attrType == AttrType.attrInteger) {
							mergedTuple.setIntFld(count, temp1.getIntFld(i+1));
						}
						else if(table1_attr[i].attrType == AttrType.attrReal) {
							mergedTuple.setFloFld(count, temp1.getFloFld(i+1));
						}
						else if(table1_attr[i].attrType == AttrType.attrString) {
							mergedTuple.setStrFld(count, temp1.getStrFld(i+1));
						}
						count++;
					}
					for(int i = 0; i < table2_len; i++) {
						if(i+1 == joinAttr2.offset) continue;
						if(table2_attr[i].attrType == AttrType.attrInteger) {
							mergedTuple.setIntFld(count, temp.t2.getIntFld(i+1));
						}
						else if(table2_attr[i].attrType == AttrType.attrReal) {
							mergedTuple.setFloFld(count, temp.t2.getFloFld(i+1));
						}
						else if(table2_attr[i].attrType == AttrType.attrString) {
							mergedTuple.setStrFld(count, temp.t2.getStrFld(i+1));
						}
						count++;
					}
					
					if(!map.containsKey(mapKey1)) {
						NRABounds nra1 = new NRABounds(merge1, null);
						nra1.t1 = temp1;
		    			objectsSeen+=1;
		    			map.put(mapKey1, nra1);
					}
					
					float v1 = 0.0f;
					float v2 = 0.0f;
					
					if(table1_attr[mergeAttr1.offset-1].attrType == AttrType.attrInteger ) {
						v1 = (float) temp1.getIntFld(mergeAttr1.offset);
					}
					else if(table1_attr[mergeAttr1.offset-1].attrType == AttrType.attrReal ) {
						v1 = temp1.getFloFld(mergeAttr1.offset);
					}
					
					if(table2_attr[mergeAttr2.offset-1].attrType == AttrType.attrInteger ) {
						v2 = (float) temp.t2.getIntFld(mergeAttr2.offset);
					}
					else if(table2_attr[mergeAttr2.offset-1].attrType == AttrType.attrReal) {
						v2 = temp.t2.getFloFld(mergeAttr2.offset);
					}
					
					mergedTuple.setFloFld(count, (v1 + v2) / (float)2.0 );
					pq.add(mergedTuple);
	 
	    		}
	    		else {
	    			NRABounds nb1 = new NRABounds(merge1, null);
	    			nb1.t1 = temp1;
	    			objectsSeen+=1;
	    			map.put(mapKey1, nb1);
	  	    	}
    		}
    		
    		if(!relation2Ended) {
	    		if(map.containsKey(joinKey2)) {
	    			NRABounds temp = map.get(joinKey2);
//					temp.t2 = temp2;
					
					Tuple mergedTuple = new Tuple();
					mergedTuple.setHdr((short) newLength, joinAttrType, joinAttrSize);
			    	int newSize = mergedTuple.size();
			    	mergedTuple = new Tuple(newSize);
			    	mergedTuple.setHdr((short) newLength, joinAttrType, joinAttrSize);
			    	
			    	count = 1;
					for(int i = 0; i < table1_len; i++) {
						if(table1_attr[i].attrType == AttrType.attrInteger) {
							mergedTuple.setIntFld(count, temp.t1.getIntFld(i+1));
						}
						else if(table1_attr[i].attrType == AttrType.attrReal) {
							mergedTuple.setFloFld(count, temp.t1.getFloFld(i+1));
						}
						else if(table1_attr[i].attrType == AttrType.attrString) {
							mergedTuple.setStrFld(count, temp.t1.getStrFld(i+1));
						}
						count++;
					}
					for(int i = 0; i < table2_len; i++) {
						if(i+1 == joinAttr2.offset) continue;
						if(table2_attr[i].attrType == AttrType.attrInteger) {
							mergedTuple.setIntFld(count, temp2.getIntFld(i+1));
						}
						else if(table2_attr[i].attrType == AttrType.attrReal) {
							mergedTuple.setFloFld(count, temp2.getFloFld(i+1));
						}
						else if(table2_attr[i].attrType == AttrType.attrString) {
							mergedTuple.setStrFld(count, temp2.getStrFld(i+1));
						}
						count++;
					}
					
					if(!map.containsKey(mapKey2)) {
						NRABounds nra2 = new NRABounds(merge2, null);
						nra2.t2 = temp2;
		    			objectsSeen+=1;
		    			map.put(mapKey2, nra2);
					}
					
					float v1 = 0.0f;
					float v2 = 0.0f;
					
					if(table1_attr[mergeAttr1.offset-1].attrType == AttrType.attrInteger ) {
						v1 = (float) temp.t1.getIntFld(mergeAttr1.offset);
					}
					else if(table1_attr[mergeAttr1.offset-1].attrType == AttrType.attrReal ) {
						v1 = temp.t1.getFloFld(mergeAttr1.offset);
					}
					
					if(table2_attr[mergeAttr2.offset-1].attrType == AttrType.attrInteger ) {
						v2 = (float) temp2.getIntFld(mergeAttr2.offset);
					}
					else if(table2_attr[mergeAttr2.offset-1].attrType == AttrType.attrReal) {
						v2 = temp2.getFloFld(mergeAttr2.offset);
					}
	    			
					mergedTuple.setFloFld(count, (v1 + v2) / (float)2.0 );
					pq.add(mergedTuple);
	    		}
	    		else {
	    			NRABounds nb2 = new NRABounds(merge2, null);
	    			nb2.t2 = temp2;
	    			objectsSeen+=1;
	    			map.put(mapKey2, nb2);
	    		}
    		}
    		
    		if(!relation1Ended) {
    			temp1 = iscan1.get_next();
    		}
    		if(!relation2Ended) {
    			temp2 = iscan2.get_next();
    		}

    	}
	}
	
	private float getMinLowerBound() {
		float min = Float.MAX_VALUE;
		
		for (Map.Entry<String,NRABounds> entry : map.entrySet()) {
			if(entry.getValue().getLowerBoundVal() < min) {
				min = entry.getValue().getLowerBoundVal();
			}
		}
		
		return min;
	}

	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		// TODO Auto-generated method stub
		Tuple res = null;
		if ( index_exist == false ) {
			return null;
		}
//		System.out.println(pq.size());
		
		if(k > 0) {
			if(pq.size() > 0) {
				res = pq.poll();
			}
//			res.print(joinAttrType);
			k--;
    	}
		
		return res;
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO Auto-generated method stub
		if ( iscan1!=null ) {
			iscan1.close();
		}
		if ( iscan2 != null ) {
			iscan2.close();
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
