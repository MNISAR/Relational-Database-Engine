package hashindex;

import global.GlobalConst;
import global.PageId;
import global.RID;
import heap.Scan;
import heap.Tuple;

public class HIndexScan implements GlobalConst {

	HashKey keyToSearch;
	Scan bucketHeapfileScan;
	RID rid;
	boolean done = false;

	public HIndexScan(HIndex hashIndex, HashKey key) throws Exception {
		this.keyToSearch = key;
		int hash = key.getHash(hashIndex.headerPage.get_H0Deapth());
		int splitPointer = hashIndex.headerPage.get_SplitPointerLocation();
		if (hash < splitPointer) {
			hash = key.getHash(hashIndex.headerPage.get_H0Deapth() + 1);
		}

		int bucketNumber = hash;
		String bucketName = hashIndex.headerPage.get_NthBucketName(bucketNumber);
		HashUtils.log("bucket of key: " + key + " is bucket: " + bucketNumber);
		HashBucket bucket = new HashBucket(bucketName);
		this.bucketHeapfileScan = bucket.heapfile.openScan();
		this.rid = new RID();
		this.done = false;
	}

	public HashEntry get_next() throws Exception {

		HashEntry ent = null;
		Tuple tup;
		while (!done) {
			tup = bucketHeapfileScan.getNext(rid);

			if (tup == null) {
				done = true;
				return ent;
			}
			HashEntry scannedHashEntry = new HashEntry(tup.returnTupleByteArray(), 0);
			if (scannedHashEntry.key.equals(keyToSearch)) {
				RID foundLocation = new RID(new PageId(scannedHashEntry.rid.pageNo.pid), scannedHashEntry.rid.slotNo);
				ent = new HashEntry(keyToSearch, foundLocation);
				break;
			}

		}

		return ent;

	}

	public void closeHIndexScan() {
		this.bucketHeapfileScan.closescan();
	}

}
