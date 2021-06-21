package tests;

import btree.BT;
import btree.BTreeFile;
import btree.BTreeSky;
import btree.FloatKey;
import btree.IndexFile;
import btree.IntegerKey;
import btree.KeyClass;
import diskmgr.PCounter;
import global.AttrType;
import global.GlobalConst;
import global.IndexType;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import index.IndexScan;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.RelSpec;

public class BTreeSkyTest implements GlobalConst {

	public final static boolean OK = true;
	public final static boolean FAIL = false;

	private BTreeFile file;
	private int keyType = AttrType.attrInteger;
	private int deleteFashion = 1;

	protected String dbpath;
	protected String logpath;



	public static void main(String[] args) {

		BTreeSkyTest thiss = new BTreeSkyTest();
		try {
			thiss.createSomeData();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public BTreeSkyTest() {
		long time=System.currentTimeMillis();
		time=10;
		dbpath = "BTREE" + time + ".minibase-db";
		logpath = "BTREE" + time + ".minibase-log";
		//SystemDefs.MINIBASE_RESTART_FLAG=true;
		SystemDefs sysdef = new SystemDefs(dbpath, 5000, 100, "Clock");

	}

	/**
	 * Create heapfile, store some data, create 3 btree indexes, run btreesky
	 */
	private void createSomeData() throws Exception {

		System.out.println("------- createSomeData ---------");

		AttrType[] attrType = new AttrType[4];
		attrType[0] = new AttrType(AttrType.attrReal);
		attrType[1] = new AttrType(AttrType.attrReal);
		attrType[2] = new AttrType(AttrType.attrReal);
		attrType[3] = new AttrType(AttrType.attrInteger);
		short[] attrSize = null;

		// create a tuple of appropriate size
		Tuple t = new Tuple();
		try {
			t.setHdr((short) attrType.length, attrType, attrSize);
		} catch (Exception e) {
			e.printStackTrace();
		}

		int size = t.size();
		t = new Tuple(size);
		try {
			t.setHdr((short) attrType.length, attrType, attrSize);
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		// Create unsorted data file "blatest.in"
		RID rid;
		Heapfile f = null;
		try {
			f = new Heapfile("blatest.in");
			//System.out.println("1. f.getRecCnt --> " + f.getRecCnt());
		} catch (Exception e) {
			e.printStackTrace();
		}

		//loop over number of record to insert
		for (int i = 0; i < 45; i++) {
			try {
				t.setFloFld(1, floatdata1[i]);
				t.setFloFld(2, fltdata2[i]);
				t.setFloFld(3, fdata3[i]);
				t.setIntFld(4, i+1000);
			} catch (Exception e) {
				e.printStackTrace();
			}
			//t.print(attrType);
			try {
				rid = f.insertRecord(t.returnTupleByteArray());
				// System.out.println("xs f.getRecCnt --> "+f.getRecCnt());

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		System.out.println("heapfile created getRecCnt --> "+f.getRecCnt()+"\n");
		System.out.println("trying to create btree file from heap file scan");

		// create an scan on the heapfile
		Scan scan = null;

		try {
			scan = new Scan(f);
		}
		catch (Exception e) {
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		// create the index file
		BTreeFile btf = null;
		try {
			btf = new BTreeFile("FirstBTreeIndex", AttrType.attrReal, 4, 1/* delete */);
		} catch (Exception e) {
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		rid = new RID();
		float key = 0;
		Tuple temp = null;

		try {
			temp = scan.getNext(rid);
		} catch (Exception e) {
			e.printStackTrace();
		}
		while (temp != null) {
			t.tupleCopy(temp);
			try {
				key = t.getFloFld(1);
				System.out.print("btree key:"+key+" ");
				t.print(attrType);
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				btf.insert(new FloatKey(key), rid);
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				temp = scan.getNext(rid);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}


		System.out.println("FirstBTreeIndex created successfully.\n");
		// close the file scan
		scan.closescan();

		//create second btree index//////////////////////////////////////////////////////////////////////
		scan = new Scan(f);
		BTreeFile btf2 =new BTreeFile("SecondBTreeIndex", AttrType.attrReal, 4, 1/* delete */);


		rid = new RID();
		key = 0;
		temp = null;

		try {
			temp = scan.getNext(rid);
		} catch (Exception e) {
			e.printStackTrace();
		}
		while (temp != null) {
			t.tupleCopy(temp);

			try {
				key = t.getFloFld(2);
				System.out.print("adding key:"+key+" ");
				t.print(attrType);
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				btf2.insert(new FloatKey(key), rid);
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				temp = scan.getNext(rid);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// close the file scan
		scan.closescan();

		System.out.println("SecondBTreeIndex created successfully.\n");
		///////////////////////////////////////////////////////////////////////////////////////////
		scan = new Scan(f);
		BTreeFile btf3 =new BTreeFile("ThirdBTreeIndex", AttrType.attrReal, 4, 1/* delete */);

		rid = new RID();
		key = 0;
		temp = null;
		try {
			temp = scan.getNext(rid);
		} catch (Exception e) {
			e.printStackTrace();
		}
		while (temp != null) {
			t.tupleCopy(temp);

			try {
				key = t.getFloFld(3);
				System.out.print("adding key:"+key+" ");
				t.print(attrType);
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				btf3.insert(new FloatKey(key), rid);
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				temp = scan.getNext(rid);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}


		scan.closescan();
		System.out.println("ThirdBTreeIndex created successfully.\n");

		/////////////////////////////Call sky fn///////

		int len_in1 = 4;
		int amt_of_mem = 100;
		Iterator am1 = null;
		String relationName = "blatest.in";
		int[] pref_list=new int[] {1,2,3};
		int pref_length_list = 3;
		IndexFile[] index_file_list = new BTreeFile[] {btf,btf2,btf3};
		int n_pages = 100;

		BTreeSky btreesky = new BTreeSky(attrType, len_in1 , /*t1_str_sizes*/attrSize, amt_of_mem, am1, relationName, pref_list, pref_length_list, index_file_list, n_pages);

		Tuple skyEle = btreesky.get_next();
		System.out.print("First Sky element is: ");
		skyEle.print(attrType);
		btreesky.close();
		System.out.println("READ COUNTER: "+PCounter.rcounter);
		System.out.println("WRITE COUNTER: "+PCounter.wcounter);
		if(true)return;

		///////////////////////////////////////////////////////////////////////////////////////////////
		//System.exit(0);
		FldSpec[] projlist = new FldSpec[3];
		RelSpec rel = new RelSpec(RelSpec.outer); 
		projlist[0] = new FldSpec(rel, 1);
		projlist[1] = new FldSpec(rel, 2);
		projlist[2] = new FldSpec(rel, 3);
		projlist[3] = new FldSpec(rel, 4);

		// start index scan
		IndexScan iscan = null;
		int keyNoForIndex = 2;
		try {
			iscan = new IndexScan(new IndexType(IndexType.B_Index), "blatest.in", "SecondBTreeIndex", attrType, attrSize, 3, 3,
					projlist, null, keyNoForIndex, false
					);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("scan creted");
		int count = 0;
		t = null;
		float outval = 0;

		try {
			t = iscan.get_next();
		} catch (Exception e) {
			e.printStackTrace();
		}
		t.print(attrType);

		boolean flag = true;

		while (t != null) {

			try {
				outval = t.getFloFld(keyNoForIndex);
				System.out.print("btree key: "+outval);
				t.print(attrType);
			} catch (Exception e) {
				e.printStackTrace();
			}


			count++;

			try {
				t = iscan.get_next();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// clean up
		try {
			iscan.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("------------------- TEST  completed ---------------------\n");



		return;

	}

	private void doo() throws Exception {

		System.out.println(" ***************** The file name is: AA  **********");
		file = new BTreeFile("AAA", keyType, 4, deleteFashion);
		file.traceFilename("TRACE");

		KeyClass key;
		RID rid = new RID();
		PageId pageno = new PageId();
		int n = 10;
		for (int i = 0; i < n; i++) {
			key = new IntegerKey(i);
			pageno.pid = i;
			rid = new RID(pageno, i);

			file.insert(key, rid);

		}
		BT.printBTree(file.getHeaderPage());
		BT.printAllLeafPages(file.getHeaderPage());

	}


	public static float[] floatdata1 = new float[] { 0.82545372f, 0.85519309f, 0.01136123f, 0.89616711f, 0.85209374f,
			0.11044171f, 0.25917021f, 0.82125408f, 0.19217470f, 0.62725607f, 0.62678302f, 0.08675860f, 0.95598429f,
			0.28701472f, 0.39743718f, 0.54008346f, 0.80160303f, 0.79776341f, 0.32459949f, 0.96526414f, 0.80194532f,
			0.36496553f, 0.63642752f, 0.06459036f, 0.27123117f, 0.45900900f, 0.45821699f, 0.78594762f, 0.13333905f,
			0.16022205f, 0.54214264f, 0.47038596f, 0.12970058f, 0.19827945f, 0.09630797f, 0.75310568f, 0.88161623f,
			0.36933813f, 0.71218063f, 0.73991992f, 0.07006711f, 0.39754960f, 0.75905891f, 0.66380499f, 0.64740700f,
			0.78964783f, 0.88224613f, 0.68972518f, 0.80575914f };

	public static float[] fltdata2 = new float[] { 0.82364352f, 0.31648985f, 0.26851860f, 0.57229880f, 0.19422482f,
			0.75878609f, 0.55002882f, 0.81404386f, 0.97576649f, 0.04374247f, 0.09585404f, 0.36646683f, 0.37973817f,
			0.29858344f, 0.47245736f, 0.44333156f, 0.80434491f, 0.13911666f, 0.25749706f, 0.25545453f, 0.03491071f,
			0.04959905f, 0.48861661f, 0.81322218f, 0.26373733f, 0.25631256f, 0.12749240f, 0.73817536f, 0.51937722f,
			0.85350038f, 0.18949572f, 0.54745234f, 0.24041191f, 0.08705491f, 0.70005883f, 0.17428024f, 0.86466622f,
			0.00360643f, 0.79316566f, 0.75659301f, 0.92752458f, 0.67531427f, 0.44842217f, 0.40936341f, 0.27186629f,
			0.33774733f, 0.38388516f, 0.52882440f, 0.63364675f };

	public static float[] fdata3 = new float[] { 0.4536858f, 0.7820702f, 0.3486266f, 0.2810114f, 0.6137574f, 0.2212215f,
			0.8377943f, 0.1048140f, 0.7610761f, 0.1338953f, 0.0482257f, 0.0735758f, 0.1000864f, 0.6025602f, 0.1631008f,
			0.6212611f, 0.7614624f, 0.8086167f, 0.6090902f, 0.1829621f, 0.2364263f, 0.9409545f, 0.9987964f, 0.5135451f,
			0.3098286f, 0.0415153f, 0.3890190f, 0.9445903f, 0.5433045f, 0.8525993f, 0.0829652f, 0.0032061f, 0.6294054f,
			0.2090061f, 0.6081148f, 0.3330478f, 0.0844854f, 0.6230508f, 0.6766866f, 0.7440832f, 0.9508495f, 0.0906593f,
			0.4670081f, 0.3313615f, 0.9605352f, 0.8187135f, 0.4744545f, 0.3083315f, 0.1928376f };
}
