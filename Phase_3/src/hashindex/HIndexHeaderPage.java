package hashindex;

import java.io.IOException;

import btree.ConstructPageException;
import diskmgr.Page;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.Tuple;

public class HIndexHeaderPage extends HFPage {

	/**
	 * this class is similar to BTreeHeaderPage class
	 * @param hashIndexName must be unique
	 */
	public HIndexHeaderPage(String hashIndexName, int numberOfBuckets) throws Exception {
		super();

		Page apage = new Page();
		PageId pageId = SystemDefs.JavabaseBM.newPage(apage, 1);
		if (pageId == null)
			throw new ConstructPageException(null, "new page failed");
		this.init(pageId, apage);

		RID hashIndexNameLocation = insertRecord(hashIndexName.getBytes());
		//HashUtils.log("hashIndexNameLocation: " + hashIndexNameLocation);
		set_NumberOfBuckets(numberOfBuckets);

	}

	public HIndexHeaderPage(PageId pageno) throws ConstructPageException {
		super();
		try {
			SystemDefs.JavabaseBM.pinPage(pageno, this, false/* Rdisk */);
		} catch (Exception e) {
			throw new ConstructPageException(e, "pinpage failed");
		}
	}

	PageId getPageId() throws IOException {
		return getCurPage();
	}

	void setPageId(PageId pageno) throws IOException {
		setCurPage(pageno);
	}
	/*
	 *number of buckets int
	 *h0 int
	 *split pointer location int  
	 */

	public void set_keyType(int keyType) throws Exception {
		setSlot(5, keyType, 0);
	}

	public int get_keyType() throws Exception {
		return getSlotLength(5);
	}

	public String get_HashIndexName() throws Exception {
		Tuple tup = getRecord(firstRecord());
		return new String(tup.getTupleByteArray());
	}

	public String get_NthBucketName(int n) throws Exception {
		return get_HashIndexName() + n;
	}

	public void set_NumberOfBuckets(int numberOfBuckets) throws IOException {
		setPrevPage(new PageId(numberOfBuckets));
	}

	public int get_NumberOfBuckets() throws IOException {
		return getPrevPage().pid;
	}
	

	public void set_H0Deapth(int location) throws IOException {
		setSlot(2, location, 0);
	}

	public int get_H0Deapth() throws IOException {
		return getSlotLength(2);
	} 

	public void set_SplitPointerLocation(int location) throws IOException {
		setSlot(3, location, 0);
	}

	public int get_SplitPointerLocation() throws IOException {
		return getSlotLength(3);
	}
	
	public void set_EntriesCount(int count) throws IOException {
		setSlot(4, count, 0);
	}

	public int get_EntriesCount() throws IOException {
		return getSlotLength(4);
	}
	
	public void set_TargetUtilization(int targetUtilization) throws IOException {
		setSlot(6, targetUtilization, 0);
	}

	public int get_TargetUtilization() throws IOException {
		return getSlotLength(6);
	}

	public void close() {
		//unpin page being done in HIndex, not here
		;
	}
}
