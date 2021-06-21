package hashindex;

import java.util.List;

import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.Tuple;

public class ClusHIndexScan implements GlobalConst {

	private List<Integer> dataFilePageNumbers;
	private RID rid;
	private boolean scandone;
	private int pageNumberTracker;
	private PageId currentPageBeingScanned;
	private HFPage currPage;
	private boolean pageDone;

	public ClusHIndexScan(ClusHIndex hashIndex, HashKey key) throws Exception {
		int hash = key.getHash(hashIndex.headerPage.get_H0Deapth());
		int splitPointer = hashIndex.headerPage.get_SplitPointerLocation();
		if (hash < splitPointer) {
			hash = key.getHash(hashIndex.headerPage.get_H0Deapth() + 1);
		}

		int bucketNumber = hash;
		String bucketName = hashIndex.headerPage.get_NthBucketName(bucketNumber);
		//HashUtils.log("bucket of key: " + key + " is bucket: " + bucketNumber);
		HashBucket bucket = new HashBucket(bucketName);

		this.rid = new RID();
		this.scandone = false;
		this.pageDone = false;
		dataFilePageNumbers = ClusHIndex.getExistingPagePointersForKeyInBucket(key, bucket);
		if (dataFilePageNumbers == null || dataFilePageNumbers.size() <= 0) {
			scandone = true;
			HashUtils.log("[ClusHIndexScan] Key not found in Clustered Hash Index");
		} else {
			pageNumberTracker = 0;
			currentPageBeingScanned = new PageId(dataFilePageNumbers.get(pageNumberTracker));
			currPage = new HFPage();

			SystemDefs.JavabaseBM.pinPage(currentPageBeingScanned, currPage, false);
			rid = currPage.firstRecord();
			if (rid == null) {
				HashUtils.log("[ClusHIndexScan] RID was null in constructor itself");
				SystemDefs.JavabaseBM.unpinPage(currentPageBeingScanned, false);
				scandone = true;
			}
		}
	}

	public Tuple get_next() throws Exception {

		if (scandone) {
			return null;
		}
		Tuple tup = currPage.getRecord(rid);

		//HashUtils.log("[ClusHIndexScan] Tup : " + tup.getLength() + " @ " + rid);
		rid = currPage.nextRecord(rid);
		if (rid == null) {
			pageDone = true;
		}
		if (pageDone) {
			SystemDefs.JavabaseBM.unpinPage(currentPageBeingScanned, false);
			pageNumberTracker++;
			if (pageNumberTracker >= dataFilePageNumbers.size()) {
				scandone = true;
			} else { // open next page with the elements
				currentPageBeingScanned = new PageId(dataFilePageNumbers.get(pageNumberTracker));
				currPage = new HFPage();
				pageDone = false;
				SystemDefs.JavabaseBM.pinPage(currentPageBeingScanned, currPage, false);
				rid = currPage.firstRecord();
			}
		}
		return tup;
	}

	public void close() throws Exception {

	}

}
