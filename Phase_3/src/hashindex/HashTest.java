package hashindex;

import java.io.File;
import java.io.IOException;

import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;

public class HashTest implements GlobalConst {

	public static void main(String[] args) {
		
		System.out.println("Start");
		
		HashTest thiss = new HashTest();
		try {
			thiss.testHIndex();
			thiss.testScan();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("End");
	}
	
	private void testScan() throws Exception {
		HIndex h = new HIndex("whatever");
		
				
		//String i = "13";
		//HashKey searchKey = new HashKey(i +"laskdhlaskdlaskhdlaskdhlaskdlaskhdalskdhlaskdhalskdhlaskdhlahdlaskdhalksaskdhaskdhaskdhlaskdhlahsd"+i);
		HashKey searchKey = new HashKey(13 );
		HIndexScan scan = h.new_scan(searchKey);
		HashEntry ent= null;
		do {
			ent = scan.get_next();
			if(ent == null) {
				System.out.println("breaking from scan loop");
				break;
			}
			System.out.println("scan.get_next(): "+ent);
		} while (ent!=null);
		h.close();
	}
	
	private void hashBla() {
		int h0 = 1;
		for (int j = 0; j < 2; j++) {
			int depth = h0+j;
			int mask = (1 << depth) - 1;
			for (int i = 0; i < 100; i++) {
				Integer value = i;

				int ikey = ((Integer) value).intValue();
				int hash = ikey & mask;
				System.out.println(value + " : "+depth+" : "  + hash);
			}
		}

	}
	
	private void testHIndex() throws Exception {
		HIndex h = new HIndex("whatever", AttrType.attrInteger, 4,15);
		for (int i = 1; i < 500; i++) {
			//HashKey key = new HashKey(i+"laskdhlaskdlaskhdlaskdhlaskdlaskhdalskdhlaskdhalskdhlaskdhlahdlaskdhalksaskdhaskdhaskdhlaskdhlahsd"+i);
			HashKey key = new HashKey(i);
			RID rid = new RID(new PageId(i),i);
			h.insert(key, rid);
		}
		h.printBucketInfo();
		System.out.println("\n DELETING ------------------------------------------------------------------\n");
		for (int i = 1; i <499; i++) {
			//HashKey key = new HashKey(i+"laskdhlaskdlaskhdlaskdhlaskdlaskhdalskdhlaskdhalskdhlaskdhlahdlaskdhalksaskdhaskdhaskdhlaskdhlahsd"+i);
			HashKey key = new HashKey(i);
			
			RID rid = new RID(new PageId(i),i);
			boolean val = h.delete(key, rid);
			if(!val)System.out.println("del"+key+"status: "+val);
		}
		
		h.printBucketInfo();
//		
//		h.delete(new HashKey(13));
//		bucket.printToConsole();
		h.close();
	}
	
	
	private void testHHeaderPage() throws Exception {
		HIndex h = new HIndex("whatever", 1, 12,80);
		printPinnedPages();
		String bucketStart = h.headerPage.get_HashIndexName();
		System.out.println("bucket start: "+bucketStart);
		System.out.println("clsing hindex...");
		h.close();
		h = null;
		h = new HIndex("whatever");
		h.close();
		
	}

	public HashTest() {
		long time=System.currentTimeMillis();
		time=10;
		
		String dbpath = "HASHTEST" + time + ".minibase-db";
		new File(dbpath).delete();
		//SystemDefs.MINIBASE_RESTART_FLAG=true;
		SystemDefs sysdef = new SystemDefs(dbpath, 5000, 100, "Clock");

	}


	private void testHashBucket() throws Exception {

		printPinnedPages();
		HashBucket bucket = new HashBucket("SomeNam");
		
		for (int i = 10; i < 30; i++) {
			HashKey key = new HashKey(i);
			RID rid = new RID(new PageId(i),i);
			HashEntry ent = new HashEntry(key, rid);
			bucket.insertEntry(ent);

		}
		bucket.printToConsole();
		HashEntry entryToDelete  = new HashEntry(new HashKey(13), new RID(new PageId(13),13));
		bucket.deleteEntry(entryToDelete);
		bucket.printToConsole();
		printPinnedPages();
		//46

	}
	
	private void testEntryCreation() throws Exception {
		HashKey key = new HashKey(12);
		RID rid = new RID(new PageId(2),3);
		HashEntry entry = new HashEntry(key, rid);
		System.out.println(""+entry);
		byte[] arr = new byte[entry.size()];
		entry.writeToByteArray(arr, 0);
		
		HashEntry deser = new HashEntry(arr, 0);
		System.out.println(""+deser);
		
	}
	
	private void testKeyCreation() throws Exception {
		HashKey key = new HashKey("blaaaaaaa");
		int keyLength = key.size();
		System.out.println(key+" keyLength: "+keyLength);
		byte[] arr = new byte[keyLength+1];
		key.writeToByteArray(arr, 1);
		key = new HashKey(arr, 1);
		System.out.println(key);
	}
	
	public static void printPinnedPages() {
		System.out.println("pin: "+(SystemDefs.JavabaseBM.getNumBuffers()- SystemDefs.JavabaseBM.getNumUnpinnedBuffers()));

	}

}
