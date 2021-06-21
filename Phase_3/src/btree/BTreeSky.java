package btree;

import java.io.IOException;
import java.util.List;

import bufmgr.PageNotReadException;
import driver.BtreeGeneratorUtil;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import iterator.BlockNestedLoopsSky;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.PredEvalException;
import iterator.SortException;
import iterator.TupleUtils;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;

public class BTreeSky extends Iterator implements GlobalConst {

	private String relationName;
	private BTreeFile[] btreeindexes;
	private AttrType[] attrType;
	private int attr_nos;
	private short[] t1_str_sizes;
	private int amt_of_mem;
	private Iterator iterator1;
	private int[] pref_list;
	private int pref_length_list;
	private int n_pages;
	public boolean debug = false;


	private String prunedHeapFileName ="btreeskyprunedfile.in";
	private Tuple firstSkyLineElement;
	private BlockNestedLoopsSky blockNestedLoopSkyline;



	/**
	 * @param in1 types of attributes in the relation
	 * @param len_in1 number of attributes in the relation
	 * @param t1_str_sizes list of string lengths for this attributes that are string type
	 * @param amt_of_mem
	 * @param am1 access method for the left iterator (may be left empty if not needed)
	 * @param relationName  heapfile filename, used to open scans
	 * @param pref_list array containing indexes of the preference attributes
	 * @param pref_length_list length of the preference list
	 * @param index_file_list array of btree index files
	 * @param n_pages number of buffer frames available for the operation
	 * @throws Exception 
	 */
	public BTreeSky(AttrType[] in1, int len_in1, short[] t1_str_sizes, int amt_of_mem, Iterator am1,
			String relationName, int[] pref_list, int pref_length_list, IndexFile[] index_file_list,
			int n_pages) throws Exception {

		this.prunedHeapFileName = System.currentTimeMillis()+".pruned";
		this.relationName = relationName;
		this.btreeindexes = (BTreeFile[]) index_file_list;

		this.attrType = in1;
		this.attr_nos = len_in1;
		this.t1_str_sizes = t1_str_sizes;
		this.amt_of_mem = amt_of_mem;
		this.iterator1 = am1;
		this.pref_list = pref_list;
		this.pref_length_list = pref_length_list;
		this.n_pages = n_pages;

		this.firstSkyLineElement = null;

		this.blockNestedLoopSkyline = null;


	}


	private void runBtreeSkyAlgo() throws Exception {
		int numberOfBtreeIndexes= btreeindexes.length;

		//create full index scans for all btrees
		BTFileScan[] fullBtreeIndexScans = new BTFileScan[numberOfBtreeIndexes];
		DiskBackedArray[] setArr = new DiskBackedArray[numberOfBtreeIndexes];
		if(debug) {
			//System.out.println("getNumUnpinnedBuffers "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
			//System.out.println("getNumBuffers "+SystemDefs.JavabaseBM.getNumBuffers());
		}
		for (int i = 0; i < numberOfBtreeIndexes; i++) {
			fullBtreeIndexScans[i] = btreeindexes[i].new_scan(null, null);
			if(debug) {
				//System.out.println("getNumUnpinnedBuffers "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
			}
			setArr[i] = new DiskBackedArray(""+i);
			if(debug) {
				//System.out.println("getNumUnpinnedBuffers "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
			}
		}
		KeyClass firstSkyEleKey = null; int btreeWithFirstSkyEle = -1;
		RID firstSkyLineElementRID = null;
		boolean stopBtreeSkyLoop =false;
		for (int skyLoopCtr = 0; /*skyLoopCtr <= 5 &&*/ stopBtreeSkyLoop  == false; skyLoopCtr++) {

			// loop over full index scans for each btree
			for (int i = 0; i < numberOfBtreeIndexes; i++) {
				KeyDataEntry scannedVal = fullBtreeIndexScans[i].get_next_entry();
				if (scannedVal == null) {
					System.out.println("got null");
					break;
				}

				// check in all other btree if this key has been found before
				RID rid = ((LeafData) scannedVal.data).getData();
				if(debug) {
					System.out.println("tree: " + i + " scannedVal: " + scannedVal.key + " RID: " + rid);
					System.out.println(i + " : " + setArr[i]);
				}
				// check in other indexes
				int foundCount=0;
				for (int otherid = 0; otherid < numberOfBtreeIndexes; otherid++) {
					if (otherid != i) {
						if(debug) {
							System.out.println("checking " + setArr[otherid]+ " otherid: "+otherid + " for " + rid);
						}
						if (setArr[otherid].getIndex(rid) >= 0) {
							if(debug) {
								System.out.println("rid found in other index" + rid);
								setArr[otherid].printToConsole();
							}

							foundCount++;
						}
					}
				}
				if (foundCount == (numberOfBtreeIndexes - 1)) {
					stopBtreeSkyLoop = true; // stop the btree skyline loop
					firstSkyLineElementRID = rid;
					firstSkyEleKey = scannedVal.key;
					btreeWithFirstSkyEle = i;
//					System.out.println("firstSkyEleKey: "+firstSkyEleKey+" btreeWithFirstSkyEle: "+btreeWithFirstSkyEle);
					if (debug) {
						System.out.println("firstSkyLineElement: " + firstSkyLineElementRID);
					}

					break;
				}

				setArr[i].add(rid);
				if (debug) {
					System.out.println("added rid"+ rid +" to diskBackedArray ");
					setArr[i].printToConsole();
					System.out.println("oooo");
				}
			}
			if (debug) {
				System.out.println("---");
			}
		} //end of btreeskyline main loop, now collect all scanned tuples and do block nested

		//open main relation data file
		Heapfile originalDataHeapFile = new Heapfile(relationName);
		firstSkyLineElement = TupleUtils.getEmptyTuple(attrType, t1_str_sizes);
		firstSkyLineElement.tupleCopy(originalDataHeapFile.getRecord(firstSkyLineElementRID));

		//create heapfile with all elements of the all the arrays of the indexes
		
		//BtreeGeneratorUtil.scanBtree(relationName,btreeindexes[btreeWithFirstSkyEle].dbname, attrType, t1_str_sizes,5,btreeWithFirstSkyEle);
		//handling duplicates on skyline
		//do handle duplicates open a scan on btree which found the first sky eleemnt
		// keep scanning till key is same as key of first sky element
		BTFileScan bla = btreeindexes[btreeWithFirstSkyEle].new_scan(firstSkyEleKey, null);
		for (boolean flag = false; flag == false; ) {
			KeyDataEntry scannedVal = bla.get_next();
			if (scannedVal == null) {
				//System.out.println("got null");
				flag = true;
				break;
			}

			RID rid = ((LeafData) scannedVal.data).getData();
			KeyClass key = scannedVal.key;
			//System.out.println("key::::::::::::::::;; "+key+" sky key:"+firstSkyEleKey);
			if (BT.keyCompare(key, firstSkyEleKey) == 0) {
				//System.out.println("heuheuehuehueheu: " + key + " rid: " + rid);
				setArr[btreeWithFirstSkyEle].add(rid);
			} else {
				flag = true;
			}
		}
		bla.DestroyBTreeFileScan();
		//create a heapfile which will store the pruned data
		Heapfile prunedDataFile = new Heapfile(prunedHeapFileName );

		//an array to check and avoid inserting duplicates in the pruned data
		DiskBackedArray insertCheckerList = new DiskBackedArray("checker");

		for (int i = 0; i < numberOfBtreeIndexes; i++) {
			DiskBackedArray curArray = setArr[i];
			if(debug) {
				System.out.print("----- pruning for array: "+i +" ----------- " );
				curArray.printToConsole();
			}
			RID temp= new RID();
			Scan scan =curArray.getHeapfile().openScan();
			Tuple tup;
			boolean done = false;
			while(!done) {
				tup= scan.getNext(temp );
				if(tup==null) {
					done=true;
					break;
				}
				RID scannedRID = DiskBackedArray.getRIDFromByteArr(tup.returnTupleByteArray());

				if(scannedRID.equals(firstSkyLineElementRID)) {
					if(debug) {
						System.out.println("not inserting any more to pruned file as skyline element hit");
					}
					break;
				}

				if(insertCheckerList.getIndex(scannedRID) < 0) { //this record not present in pruned file
					Tuple mainFileTuple = originalDataHeapFile.getRecord(scannedRID);

					prunedDataFile.insertRecord(mainFileTuple.getTupleByteArray() );
					insertCheckerList.add(scannedRID);
					if(debug) {
						System.out.println("inserted record to pruned data file");
					}
				} else {
					if(debug) {
						System.out.println("not inserting record as already present in pruned file");
					}
				}

			}
			if(debug) {
				System.out.println("");
			}



			scan.closescan();

		}
		
//		System.out.println("Will now run Block Nested Loop Skyline on \n Pruned DataFile record count: "+prunedDataFile.getRecCnt());
		
		//run block nested loop skyline on the pruned data now
		
		//close all btree scans and btree index files
		for (int i = 0; i < numberOfBtreeIndexes; i++) {
			fullBtreeIndexScans[i].DestroyBTreeFileScan();
			btreeindexes[i].close();
			setArr[i].close();
			
		}
		insertCheckerList.close();
		
		if(debug) {
			System.out.println("getNumUnpinnedBuffers "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
			System.out.println("getNumBuffers "+SystemDefs.JavabaseBM.getNumBuffers());
		}
		//unlimit the buffer manager here, it will be limited again in BNL
		//SystemDefs.JavabaseBM.limit_memory_usage(false, this.n_pages);
		//SystemDefs.JavabaseBM.flushAllPages();
		Iterator bnlIterator = null;
		blockNestedLoopSkyline = new BlockNestedLoopsSky(attrType, attrType.length, t1_str_sizes, bnlIterator, prunedHeapFileName, pref_list, pref_length_list, n_pages);
	}



	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
	InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
	LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {

		if(firstSkyLineElement == null) { //run btreesky
			runBtreeSkyAlgo();
			return firstSkyLineElement;

		} else { //run block nested loop sky
			return blockNestedLoopSkyline.get_next();
		}
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		try {
			new Heapfile(prunedHeapFileName).deleteFile();
		} catch (Exception e) {
			e.printStackTrace();
		}
		blockNestedLoopSkyline.close();
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