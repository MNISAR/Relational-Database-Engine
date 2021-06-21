package tests;

import btree.BT;
import btree.BTFileScan;
import btree.BTreeFile;
import btree.DataClass;
import btree.FloatKey;
import btree.IntegerKey;
import btree.KeyClass;
import btree.KeyDataEntry;
import btree.StringKey;
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
import iterator.RelSpec;

public class BTreeFloatTest implements GlobalConst {

	public final static boolean OK = true;
	public final static boolean FAIL = false;

	private BTreeFile file;
	private int keyType = AttrType.attrInteger;
	private int deleteFashion = 1;
	
	protected String dbpath;
	protected String logpath;

	private float[] floatdata = new float[] {0.82545372f,
			0.85519309f,
			0.01136123f,
			0.89616711f,
			0.85209374f,
			0.11044170f,
			0.25917021f,
			0.82125408f,
			0.19217470f,
			0.62725607f,
			0.62678302f,
			0.08675860f,
			0.95598429f,
			0.28701472f,
			0.39743718f,
			0.54008346f,
			0.80160303f,
			0.79776341f,
			0.32459949f,
			0.12455648f};

	public static void main(String[] args) {

		BTreeFloatTest thiss = new BTreeFloatTest();
		try {
			thiss.createSomeData();
			//thiss.scanSomeData();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void scanSomeData() {
		System.out.println("------- scanSomeData ---------");
		// create an scan on the heapfile
		System.out.println("will try to scan data");
		try {
			AttrType[] attrType = new AttrType[2];
			attrType[0] = new AttrType(AttrType.attrReal);
			attrType[1] = new AttrType(AttrType.attrInteger);

			Tuple t = new Tuple();

			t.setHdr((short) 2, attrType, null);

			int size = t.size();
			t = new Tuple(size);
			t.setHdr((short) 2, attrType, null);



			Heapfile f = new Heapfile("blatest.in");
			System.out.println("Heapfile.getRecCnt();-->"+f.getRecCnt());
			Scan scan = new Scan(f);

			RID rid = new RID();
			Tuple temp = null;
			temp=scan.getNext(rid );

			while(temp!=null) {
				t.tupleCopy(temp);
				t.print(attrType);
				temp=scan.getNext(rid);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void createSomeData() throws Exception {

		System.out.println("------- createSomeData ---------");

		AttrType[] attrType = new AttrType[2];
		attrType[0] = new AttrType(AttrType.attrReal);
		attrType[1] = new AttrType(AttrType.attrInteger);
		short[] attrSize = null;

		// create a tuple of appropriate size
		Tuple t = new Tuple();
		try {
			t.setHdr((short) 2, attrType, attrSize);
		} catch (Exception e) {
			e.printStackTrace();
		}

		int size = t.size();
		t = new Tuple(size);
		try {
			t.setHdr((short) 2, attrType, attrSize);
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
		for (int i = 0; i < 20; i++) {
			try {
				t.setFloFld(1, floatdata[i]);
				t.setIntFld(2, 1000+i);
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
			t.print(attrType);
			try {
				key = t.getFloFld(1);
				System.out.println("key:"+key);
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

		// close the file scan
		scan.closescan();


		System.out.println("FirstBTreeIndex created successfully.\n");
		//System.exit(0);
		FldSpec[] projlist = new FldSpec[2];
		RelSpec rel = new RelSpec(RelSpec.outer); 
		projlist[0] = new FldSpec(rel, 1);
		projlist[1] = new FldSpec(rel, 2);

		// start index scan
		IndexScan iscan = null;
		try {
			iscan = new IndexScan(new IndexType(IndexType.B_Index), "blatest.in", "FirstBTreeIndex", attrType, attrSize, 2, 2,
					projlist, null, 2, false
					);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("scan creted");
		int count = 0;
		t = null;
		int outval = 0;

		try {
			t = iscan.get_next();
		} catch (Exception e) {
			e.printStackTrace();
		}
		t.print(attrType);

		boolean flag = true;

		while (t != null) {

			try {
				outval = t.getIntFld(1);
				System.out.println("outval: "+outval);
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

	public BTreeFloatTest() {
		long time=System.currentTimeMillis();
		time=10;
		dbpath = "BTREE" + time + ".minibase-db";
		logpath = "BTREE" + time + ".minibase-log";
		//SystemDefs.MINIBASE_RESTART_FLAG=true;
		SystemDefs sysdef = new SystemDefs(dbpath, 300, NUMBUF, "Clock");

	}
}
