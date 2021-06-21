package driver;

import java.io.IOException;

import btree.BTreeFile;
import btree.FloatKey;
import global.AttrType;
import global.IndexType;
import global.RID;
import global.SystemDefs;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import index.IndexScan;
import iterator.FldSpec;
import iterator.RelSpec;

public class BtreeGeneratorUtil {

	private BtreeGeneratorUtil() {}
	
	/**
	 * Generate Btree indexes for <b>all attributes indiviually</b>  present in the heapfile<br>
	 * The btree indexes have a {@link FloatKey} type key<br>
	 * DOESNT CREATE COMBINED BTREE INDEX, only multiple btrees index on individual column
	 * @param pref_list 
	 */
	public static void generateAllBtreesForHeapfile(String relationName,Heapfile heapFile,AttrType[] attrType,short[] t1_str_sizes) throws Exception {

		
		int COLS =attrType.length;
		BTreeFile[] btreeFileArray = new BTreeFile[COLS];
		
		for(int i =0;i<COLS;i++) {
			String btreename = "BTreeIndex"+(i+1);
			btreeFileArray[i] = createSingleBtree(btreename , new Scan(heapFile), attrType, t1_str_sizes,(i+1));
			btreeFileArray[i].close();
		}

		//scanBtree(relationName,"BTreeIndex4", attrType, t1_str_sizes,5,4);

		SystemDefs.JavabaseBM.flushAllPages();
	}

	/**
	 * get only the btree indexes specified by pref_list<br>
	 * just opens the previously created btree files
	 * @param pref_list attributes number to get Btree for
	 */
	public static BTreeFile[] getBtreeSubset(int[] pref_list) throws Exception{
		BTreeFile[] btreeFileArray = new BTreeFile[pref_list.length];
		for(int i =0;i<pref_list.length;i++) {
			int prefAtrr = pref_list[i];
			btreeFileArray[i]= new BTreeFile("BTreeIndex"+prefAtrr);
		}
		
		return btreeFileArray;
	}

	private static BTreeFile createSingleBtree(String btreeName,Scan scan, AttrType[] attrType,short[] t1_str_sizes,int keyNoForIndex) throws Exception {

		// create the index file
		BTreeFile btf  = new BTreeFile(btreeName, AttrType.attrReal, 4, 1/* delete */);


		RID rid = new RID();
		Tuple t = getEmptyTuple(attrType,t1_str_sizes);
		float key = 0;
		
		Tuple temp = scan.getNext(rid);
		
		while (temp != null) {
			t.tupleCopy(temp);

			key = -1 * t.getFloFld(keyNoForIndex);
			//System.out.print("btree key:"+key+" ");
			//t.print(attrType);

			btf.insert(new FloatKey(key), rid);

			temp = scan.getNext(rid);

		}
		scan.closescan();
		
		//System.out.println(btreeName+" created successfully.\n");
		
		return btf;


	}

	/**
	 * Utility function to see the contents of a btree index and associated tuples<br>
	 */
	private static void scanBtree(String relationName,String btreeName, AttrType[] attrType, short[] attrSize,int numberOfFileds,int keyNoForIndex) throws Exception {
		FldSpec[] projlist = new FldSpec[5];
		RelSpec rel = new RelSpec(RelSpec.outer);
		projlist[0] = new FldSpec(rel, 1);
		projlist[1] = new FldSpec(rel, 2);
		projlist[2] = new FldSpec(rel, 3);
		projlist[3] = new FldSpec(rel, 4);
		projlist[4] = new FldSpec(rel, 5);

		// start index scan
		IndexScan iscan = null;


		iscan = new IndexScan(new IndexType(IndexType.B_Index), relationName, btreeName, attrType, attrSize, numberOfFileds,
				numberOfFileds, projlist, null, keyNoForIndex, false);

		System.out.println("scan creted");
		Tuple t = null;
		float outval = 0;

		t = iscan.get_next();
		while (t != null) {

			outval = t.getFloFld(keyNoForIndex);
			System.out.print("btree key: " + outval +" tuple: ");
			t.print(attrType);


			t = iscan.get_next();

		}

		iscan.close();

	}

	private static Tuple getEmptyTuple(AttrType[] attrType, short[] t1_str_sizes) throws InvalidTypeException, InvalidTupleSizeException, IOException {
		Tuple t = new Tuple();
		t.setHdr((short) attrType.length, attrType, t1_str_sizes);
		int size = t.size();
		t = new Tuple(size);
		t.setHdr((short) attrType.length, attrType, t1_str_sizes);
		return t;
	}


}
