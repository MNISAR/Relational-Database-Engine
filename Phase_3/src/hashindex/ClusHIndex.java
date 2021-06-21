package hashindex;

import java.util.ArrayList;
import java.util.List;

import btree.KeyNotMatchException;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import hashindex.HashUtils.Pair;
import heap.ClusHIndexDataFile;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;

public class ClusHIndex implements GlobalConst{

	HIndexHeaderPage headerPage;
	PageId headerPageId;
	ClusHIndexDataFile dataFile;

	final float targetUtilization;


	public ClusHIndex(String datafilename, String indexfileName, int keyType, int keySize,int targetUtilization) throws Exception {

		headerPageId = HashUtils.get_file_entry(indexfileName);
		if (headerPageId == null) // file not exist
		{
			HashUtils.log("Creating new HIndex header page");
			//creating new header page with filename and number of buckets 2
			headerPage = new HIndexHeaderPage(indexfileName,2);
			headerPageId = headerPage.getPageId();
			HashUtils.add_file_entry(indexfileName, headerPageId);

			headerPage.set_keyType( keyType);
			headerPage.set_H0Deapth(1);
			headerPage.set_SplitPointerLocation(0);
			headerPage.set_EntriesCount(0);
			headerPage.set_TargetUtilization(targetUtilization);


		} else {
			HashUtils.log("Opening existing HIndex");
			headerPage = new HIndexHeaderPage(headerPageId);
		}
		this.targetUtilization = (float) ((float)headerPage.get_TargetUtilization()/100.0);
		this.dataFile = new ClusHIndexDataFile(datafilename);


	}

	public ClusHIndex(String datafilename, String indexfileName) throws Exception {
		headerPageId = HashUtils.get_file_entry(indexfileName);
		if(headerPageId == null) {
			throw new IllegalArgumentException("No index found with name "+indexfileName);
		}
		headerPage = new HIndexHeaderPage(headerPageId);
		this.targetUtilization = (float) ((float)headerPage.get_TargetUtilization()/100.0);
		this.dataFile = new ClusHIndexDataFile(datafilename);
	}

	public RID insert(HashKey key,Tuple tup) throws Exception {
		HashUtils.log("[ClusHIndex] trying to insert key : "+key);

		if (key.type != headerPage.get_keyType()) {
			throw new KeyNotMatchException("Key types dont match!");
		}
		int hash = key.getHash(headerPage.get_H0Deapth());
		int splitPointer = headerPage.get_SplitPointerLocation();
		if (hash < splitPointer) {
			//System.out.println("old hash: " + hash);
			hash = key.getHash(headerPage.get_H0Deapth() + 1);
			//System.out.println("new hash: " + hash);
		}

		int bucketNumber = hash;
		String bucketName = headerPage.get_NthBucketName(bucketNumber);
		HashBucket bucket = new HashBucket(bucketName);


		//insert data in datafile and key in bucket(if reqd)
		RID insertedLocation = insertInDataFileAndBucket(key, tup, bucket);


		// now add buckets(pages) if reqd
		float currentEntryCount = headerPage.get_EntriesCount();
		int bucketCount = headerPage.get_NumberOfBuckets();
		float maxPossibleEntries = (bucketCount * MINIBASE_PAGESIZE) / (8+key.size());
		float currentUtilization = currentEntryCount / maxPossibleEntries;
		HashUtils.log("currentUtilization: " + currentUtilization);
		HashUtils.log("targetUtilization: " + targetUtilization);
		if (currentUtilization >= targetUtilization) {
			HashUtils.log("Adding a bucket page to HIndex");
			//System.out.println("currentUtilization: " + currentUtilization);
			headerPage.set_NumberOfBuckets(headerPage.get_NumberOfBuckets() + 1);
			// rehash element in bucket splitPointer

			rehashClusBucket(headerPage.get_NthBucketName(splitPointer), headerPage.get_H0Deapth() + 1);
			splitPointer++;
			if (splitPointer == (1 << headerPage.get_H0Deapth())) {
				splitPointer = 0;
				headerPage.set_H0Deapth(headerPage.get_H0Deapth() + 1);
				HashUtils.log("resetting split pointer to 0 ");
			}
			headerPage.set_SplitPointerLocation(splitPointer);
			HashUtils.log("after split splitPointer: " + splitPointer);

		}
		HashUtils.log("[ClusHIndex] inserted key "+key+" @ "+ insertedLocation );
		return insertedLocation;
	}

	/**
	 * Delete all duplicates of tuple from the clustered hash index data file <br>
	 * if the entire page of data file gets deleted then delete entry from index too <br>
	 * Will shrink the index if utilization goes < (targetUtilization - 10%)
	 * @param key of the tuple in index
	 * @param tup record to be deleted from data file
	 * @return list of key,location of the record in the data file which was deleted
	 */
	public List<RID> delete(HashKey key, Tuple tup) throws Exception {
		int hash = key.getHash(headerPage.get_H0Deapth());
		int splitPointer = headerPage.get_SplitPointerLocation();
		if (hash < splitPointer) {
			hash = key.getHash(headerPage.get_H0Deapth() + 1);
			//System.out.println("new hash: " + hash);
		}

		int bucketNumber = hash;
		String bucketName = headerPage.get_NthBucketName(bucketNumber);
		HashBucket bucket = new HashBucket(bucketName);
		//System.out.println("Will try to delete key: "+key+" from bucket: "+hash + " sp: "+splitPointer);

		List<RID> list=deleteFromDataFileAndBucket(key,tup,bucket);
		
		//now shrink index if reqd
		float currentUtilization = headerPage.get_EntriesCount() / ((headerPage.get_NumberOfBuckets() * MINIBASE_PAGESIZE) / (8+key.size()));
		HashUtils.log("currentUtilization: " + currentUtilization);
		float deletionTargetUtilization = (float) (targetUtilization - 0.1 < 0 ? 0 : targetUtilization - 0.1) ;
		HashUtils.log("deletionTargetUtilization: " + deletionTargetUtilization);
		//System.out.println("utiliztion status "+currentUtilization+"<"+deletionTargetUtilization);
		//KEEP SHRINKING THE INDEX TILL UTILIZATION IS WHAT WE WANT
		while (headerPage.get_NumberOfBuckets() > 2 && currentUtilization <= deletionTargetUtilization) {
			//System.out.println("Shrinking the index as "+currentUtilization+"<"+deletionTargetUtilization);
			//System.out.println("shrink: "+headerPage.get_NumberOfBuckets()+" sp: "+splitPointer+" h0:"+headerPage.get_H0Deapth());
			if(splitPointer == 0) {
				rehashClusBucket(headerPage.get_NthBucketName(headerPage.get_NumberOfBuckets()-1), headerPage.get_H0Deapth()-1);
			} else {
				rehashClusBucket(headerPage.get_NthBucketName(headerPage.get_NumberOfBuckets()-1), headerPage.get_H0Deapth());
			}
			splitPointer --;
			if (splitPointer == -1) {
				headerPage.set_H0Deapth(headerPage.get_H0Deapth() - 1);
				splitPointer = (1 << headerPage.get_H0Deapth()) - 1;
				HashUtils.log("resetting split pointer to  "+splitPointer);
			}
			headerPage.set_NumberOfBuckets(headerPage.get_NumberOfBuckets()-1);
			headerPage.set_SplitPointerLocation(splitPointer);
			//System.out.println("after shrink: "+headerPage.get_NumberOfBuckets()+" sp: "+splitPointer+" h0:"+headerPage.get_H0Deapth());
			currentUtilization = headerPage.get_EntriesCount() / ((headerPage.get_NumberOfBuckets() * MINIBASE_PAGESIZE) / (8+key.size()));
		}
		return list;
	}

	private List<RID> deleteFromDataFileAndBucket(HashKey key, Tuple tup, HashBucket bucket) throws Exception {
		byte[] record = tup.getTupleByteArray();
		List<Integer> pageNumList = getExistingPagePointersForKeyInBucket(key,bucket);
		List<RID> list = new ArrayList<>();

		for (Integer pageNumOfKeyInDataFile : pageNumList) {
			Pair<List<RID>, Boolean> deletionInfo = dataFile.deleteRecordFromPage(record, new PageId(pageNumOfKeyInDataFile));
			List<RID> deletedRecordsRIDs = deletionInfo.first;
			boolean isDataPageDeleted = deletionInfo.second;
			HashUtils.log("Deleted "+deletedRecordsRIDs.size()+" records from data file page: "+pageNumOfKeyInDataFile+" data page deleted?:"+isDataPageDeleted);
			if(isDataPageDeleted) {
				bucket.deleteEntry(new HashEntry(key, new RID(new PageId(pageNumOfKeyInDataFile), 0)));
				headerPage.set_EntriesCount(headerPage.get_EntriesCount() - 1);
			}
			list.addAll(deletedRecordsRIDs);
		}
		return list;
	}

	public void close() throws Exception {
		if (headerPage != null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}
	}

	private RID insertInDataFileAndBucket(HashKey key, Tuple tup, HashBucket bucket) throws Exception {
		byte[] record = tup.getTupleByteArray();
		List<Integer> pageNumList = getExistingPagePointersForKeyInBucket(key,bucket);
		RID insertedLocationOfTupleInDataFile = null;
		boolean recordInsertedInDataFile = false;
		//first try to insert in any of the pages which have same key
		for (Integer pageNumOfKeyInDataFile : pageNumList) {
			RID locationInDataFile = dataFile.insertRecordOnExistingPage(record, new PageId(pageNumOfKeyInDataFile));

			if(locationInDataFile != null) {
				insertedLocationOfTupleInDataFile = new RID(new PageId(locationInDataFile.pageNo.pid),locationInDataFile.slotNo);
				recordInsertedInDataFile = true;
				HashUtils.log("[ClusHIndex][SAME_KEY] Inserted data in " + key + " to existing bucket entry, RID in data file: " + insertedLocationOfTupleInDataFile);
				break;
			}
		}

		if(recordInsertedInDataFile == false) { //insert data to new page in datafile, key in bucket
			RID loc = dataFile.insertRecordToNewPage(record);
			insertedLocationOfTupleInDataFile = new RID(new PageId(loc.pageNo.pid),loc.slotNo);
			HashEntry ent = new HashEntry(key, new RID(new PageId(insertedLocationOfTupleInDataFile.pageNo.pid),0)); 
			bucket.insertEntry(ent);
			headerPage.set_EntriesCount(headerPage.get_EntriesCount() + 1);
			HashUtils.log("[ClusHIndex][NEW_KEY] Inserting " + key + " to bucket: " + bucket);

		} 
		return insertedLocationOfTupleInDataFile;
	}

	public static List<Integer> getExistingPagePointersForKeyInBucket(HashKey key,HashBucket buc) throws Exception {
		List<Integer> pageNumList = new ArrayList<>();
		Scan scan = buc.heapfile.openScan();
		RID rid = new RID();
		Tuple tup;
		boolean done = false;
		while (!done) {
			tup = scan.getNext(rid);

			if (tup == null) {
				done = true;
				break;
			}
			HashEntry scannedHashEntry = new HashEntry(tup.returnTupleByteArray(), 0);
			if(scannedHashEntry.key.equals(key)) {
				int pageNum = scannedHashEntry.rid.pageNo.pid;
				//HashUtils.log("Key is already in bucket with page pointer: "+pageNum);
				pageNumList.add(pageNum);
			}
		}
		scan.closescan();
		return pageNumList;
	}

	private void rehashClusBucket(String bucketToBeRehashedName,int newDeapth) throws Exception {
		//System.out.println("bucketToBeRehashedName "+bucketToBeRehashedName+",newDeapth:"+newDeapth);
		Heapfile tempheapfile = new Heapfile("kjdgaslkdalskjdtemp");
		HashBucket bucketToBeRehashed = new HashBucket(bucketToBeRehashedName);
		Scan scan = bucketToBeRehashed.heapfile.openScan();
		RID rid = new RID();
		Tuple tup;
		boolean done = false;
		int i = 0;
		while (!done) {
			tup = scan.getNext(rid);

			if (tup == null) {
				done = true;
				break;
			}
			tempheapfile.insertRecord(tup.returnTupleByteArray());
			i++;
		}
		//System.out.println("entries added to temp heapfile: "+i);
		scan.closescan();
		bucketToBeRehashed.heapfile.deleteFile();
		Scan tempHeapScan = tempheapfile.openScan();
		bucketToBeRehashed = new HashBucket(bucketToBeRehashedName);
		rid = new RID();
		done = false;
		i = 0;
		while (!done) {
			tup = tempHeapScan.getNext(rid);

			if (tup == null) {
				done = true;
				break;
			}
			HashEntry scannedHashEntry = new HashEntry(tup.returnTupleByteArray(), 0);
			int hash1 = scannedHashEntry.key.getHash(newDeapth);
			String newBucketName = headerPage.get_NthBucketName(hash1);
			HashBucket newBucket = new HashBucket(newBucketName );
			newBucket.insertEntry(scannedHashEntry);
			//System.out.println("Rehashing "+scannedHashEntry.key+" to bucket "+newBucketName);

			i++;
		}
		//System.out.println("entries rehashed: " + i);
		tempHeapScan.closescan();
		tempheapfile.deleteFile();
	}

	/**
	 * Create an equality scan for the key<br>
	 * use this to search the index for a key
	 * @param key
	 */
	public ClusHIndexScan new_scan(HashKey key) throws Exception {
		ClusHIndexScan scan = new ClusHIndexScan(this, key);
		return scan;
	}

	public HIndexHeaderPage getHeaderPage() {
		return headerPage;
	}
	public ClusHIndexDataFile getDataFile() {
		return dataFile;
	}

	/**
	 * just print the contents of the buckets, ie the key,pageId pairs in each bucket
	 */
	public void printBucketInfo() {
		try {
			for( int i=0; i<this.headerPage.get_NumberOfBuckets(); i++ ) {
				HashBucket bucket = new HashBucket(this.headerPage.get_NthBucketName(i));

				Scan scan = bucket.heapfile.openScan();
				RID rid = new RID();
				Tuple tup;
				int count = 0;
				boolean done = false;
				System.out.println("BUCKET: "+i+" HashBucket [ ");
				while (!done) {
					tup = scan.getNext(rid);
					if (tup == null) {
						done = true;
						break;
					}
					HashEntry scannedHashEntry = new HashEntry(tup.returnTupleByteArray(), 0);
					//System.out.println("  " + scannedHashEntry + " @ IN-BUCKET LOCATION:  " + rid);
					System.out.println("  <"+scannedHashEntry.key.value+","+scannedHashEntry.rid.pageNo.pid+">");
					count++;
				}
				System.out.println("] count: " + count);
				scan.closescan();


			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
