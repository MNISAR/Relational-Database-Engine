package heap;


import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import hashindex.ClusHIndex;
import hashindex.ClusHIndexScan;
import hashindex.HashBucket;
import hashindex.HashKey;
import hashindex.HashUtils;
import hashindex.HashUtils.Pair;

public class TestHashClusDataFileTest implements GlobalConst {

	public static void main(String[] args) {
		
		System.out.println("Start");
		
		TestHashClusDataFileTest thiss = new TestHashClusDataFileTest();
		try {
			thiss.testInsert();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("End");
	}
	
	private void testInsert() throws Exception {
		String INDEX_NAME = "huehuehue";
		ClusHIndex index = new ClusHIndex("clhdf"+INDEX_NAME, INDEX_NAME,AttrType.attrInteger, 4,15);
		System.out.println("\n\nNOW INSERTING SOME DATA IN INDEX \n");
		int N = 150;
		for (int k = 100; k < N; k++) {
			HashUtils.log("------------------------------------------------------");

			HashKey key= new HashKey(k);
			HashKey data= new HashKey(k);

			//HashKey key = new HashKey(k);
			byte[] arr = new byte[data.size()];
			data.writeToByteArray(arr, 0);

			Tuple tup = new Tuple(arr, 0, arr.length);
			for (int i = 0; i < 100; i++) {
				HashUtils.log("\nooooooooooooooo");
				
				RID loc = index.insert(key, tup);
//				printPinnedPages();
			}
			//data= new HashKey(200+k);data.writeToByteArray(arr, 0);tup = new Tuple(arr, 0, arr.length);
			//index.insert(key, tup);
			HashUtils.log("------------------------------------------------------\n");
		}
		//System.out.println(index.getHeaderPage().get_EntriesCount());
		
		System.out.println("\n\nPRINTING ALL BUCKETS OF THE INDEX \n");
		index.printBucketInfo();
		System.out.println("\n\nPRINTING THE DATA FILE \n");
		index.getDataFile().printToConsole(mapper);
		index.close();
		index = new ClusHIndex("clhdf"+INDEX_NAME, INDEX_NAME);
		System.out.println("\n\nNOW TESTING SCAN OF INDEX WITH SEARCH KEY \n");
		ClusHIndexScan scan = index.new_scan(new HashKey(13));
		Tuple tup= null;
		do {
			tup = scan.get_next();
			if(tup == null) {
				System.out.println("breaking from scan loop");
				break;
			}
			System.out.println("scan.get_next(): "+mapper.apply(tup.getTupleByteArray()));
		} while (tup!=null);
		
		
		System.out.println("\n\n DELETING SOME KEYS \n");
		for (int k = 100; k < N-1; k++) {
			//System.out.println("------------------------------------------------------");
			//System.out.println("K:"+k);
			HashKey key= new HashKey(k);

			//HashKey key = new HashKey(k);
			byte[] arr = new byte[key.size()];
			key.writeToByteArray(arr, 0);

			Tuple tup1 = new Tuple(arr, 0, arr.length);
			List<RID> val = index.delete(key, tup1);
			if(val.size()<=0)System.out.println("for key: "+key+"deleted RID list: "+val);
			//System.out.println("------------------------------------------------------\n");
		}
		System.out.println("\n\nPRINTING BUCKETS \n");
		index.printBucketInfo();
		System.out.println("\n\nPRINTING THE DATA FILE \n");
		index.getDataFile().printToConsole(mapper);
//		index.getDataFile().printToConsole(mapper);
		//		index.delete(key, tup)
		index.close();
	}
	public static String generateRandomChars( int length) {
		String candidateChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
	    StringBuilder sb = new StringBuilder();
	    java.util.Random random = new java.util.Random();
	    for (int i = 0; i < length; i++) {
	        sb.append(candidateChars.charAt(random.nextInt(candidateChars
	                .length())));
	    }

	    return sb.toString();
	}
	
	private void testFile() throws Exception {
		ClusHIndexDataFile file = new ClusHIndexDataFile("whateveer");
		for(int k =10;k<15;k++) {
			HashKey key = new HashKey(k);
			byte[] arr= new byte[key.size()];
			key.writeToByteArray(arr, 0);
			RID loc  = file.insertRecordToNewPage(arr);
			for (int i = 0; i < 2; i++) {
				file.insertRecordOnExistingPage(arr, loc.pageNo);

			}
			key = new HashKey(k+100);key.writeToByteArray(arr, 0);
			file.insertRecordOnExistingPage(arr, loc.pageNo);
		}
		
		file.printToConsole(mapper);
		HashKey key = new HashKey(11);byte[] record= new byte[key.size()];key.writeToByteArray(record, 0);
		Boolean isDatPageDel = false;
		Pair<List<RID>, Boolean> val = file.deleteRecordFromPage(record, new PageId(5) );
		System.out.println("is data page del: "+val.second);
		file.printToConsole(mapper);
//		Scan scan = file.openScan();
//		RID rid = new RID();
//		Tuple tup;
//		boolean done = false;
//		while (!done) {
//			tup = scan.getNext(rid);
//
//			if (tup == null) {
//				done = true;
//				break;
//			}
//			HashKey scannedHashKey = new HashKey(tup.returnTupleByteArray(), 0);
//			HashUtils.log("Location: "+rid+" - "+scannedHashKey);
//
//		}
//		scan.closescan();

	}


	Function<byte[],String> mapper= (a)->{
		try {
			return new HashKey(a, 0).toString();
		}catch (Exception e) {
			e.printStackTrace();
			return e.getMessage();
		}
	};

	public TestHashClusDataFileTest() {
		long time=System.currentTimeMillis();
		time=10;
		String dbpath = "HASHTEST" + time + ".minibase-db";
		File file = new File(dbpath);
		System.out.println("file.delete(): "+file.delete());;
		//SystemDefs.MINIBASE_RESTART_FLAG=true;
		SystemDefs sysdef = new SystemDefs(dbpath, 5000, 100, "Clock");

	}



	
	public static void printPinnedPages() {
		System.out.println("pin: "+(SystemDefs.JavabaseBM.getNumBuffers()- SystemDefs.JavabaseBM.getNumUnpinnedBuffers()));

	}

}

