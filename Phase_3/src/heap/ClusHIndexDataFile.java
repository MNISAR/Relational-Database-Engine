package heap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import diskmgr.Page;
import global.PageId;
import global.RID;
import hashindex.HashUtils;
import hashindex.HashUtils.Pair;

public class ClusHIndexDataFile extends Heapfile {

	public ClusHIndexDataFile(String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
		super(name);
		// HashUtils.log("Created ClustDataFile" + name);
	}

	public RID insertRecordOnExistingPage(byte[] record, PageId pageToInsertId) throws Exception {

		int recLen = record.length;
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);

		PageId nextDirPageId = new PageId(0);

		HFPage currentDirPage = new HFPage();
		RID ridOfPageInDirPage = null;
		boolean found = false;
		boolean canBeStored = false;
		DataPageInfo dpinfo = new DataPageInfo();
		while (currentDirPageId.pid != INVALID_PAGE) {
			// HashUtils.log("checking dir page id :"+currentDirPageId.pid);
			pinPage(currentDirPageId, currentDirPage, false);

			RID rid = new RID();
			Tuple atuple;
			for (rid = currentDirPage.firstRecord(); rid != null; // rid==NULL means no more record
					rid = currentDirPage.nextRecord(rid)) {
				atuple = currentDirPage.getRecord(rid);
				dpinfo = new DataPageInfo(atuple);

				if (dpinfo.pageId.pid == pageToInsertId.pid) {
					// HashUtils.log("page id found in directory "+dpinfo.pageId.pid);
					found = true;
					if (recLen <= dpinfo.availspace) {
						// HashUtils.log("data page has enough space");
						ridOfPageInDirPage = new RID(new PageId(rid.pageNo.pid), rid.slotNo);
						canBeStored = true;
					} else {
						// HashUtils.log("not enough space in data page :"+pageToInsertId.pid);
						canBeStored = false;
					}

					break;
				}

			}

			unpinPage(currentDirPageId, false /* undirty */);
			if (found == true) {
				// HashUtils.log("breaking from outer loop");
				break;
			}
			nextDirPageId = currentDirPage.getNextPage();
			currentDirPageId.pid = nextDirPageId.pid;
		}

		if (canBeStored == false) { // not possible to insert in this data page
			return null;
		}
		HFPage dataPageToInsert = new HFPage();
		pinPage(pageToInsertId, dataPageToInsert, false);

		if ((dpinfo.pageId).pid == INVALID_PAGE) // check error!
			throw new HFException(null, "invalid PageId");

		if (!(dataPageToInsert.available_space() >= recLen))
			throw new SpaceNotAvailableException(null, "no available space");

		RID rid;
		rid = dataPageToInsert.insertRecord(record);
		dpinfo.recct++;
		dpinfo.availspace = dataPageToInsert.available_space();
		dpinfo.flushToTuple();
		unpinPage(dpinfo.pageId, true /* = DIRTY */);

		pinPage(currentDirPageId, currentDirPage, false);
		DataPageInfo dpinfo_ondirpage = new DataPageInfo(currentDirPage.returnRecord(ridOfPageInDirPage));

		dpinfo_ondirpage.availspace = dpinfo.availspace;
		dpinfo_ondirpage.recct = dpinfo.recct;
		dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;
		dpinfo_ondirpage.flushToTuple();

		unpinPage(currentDirPageId, true /* = DIRTY */);

		return rid;

	}

	public RID insertRecordToNewPage(byte[] record) throws Exception {
		// create new page and insert data to it

		DataPageInfo newPageInfo = new DataPageInfo();
		HFPage newDataPage = _newDatapage(newPageInfo);

		// have commented the below line as _newDatapage already pins the page
		// pinPage(newPageInfo.pageId, newDataPage, false);

		RID insertedRecordLocation;
		insertedRecordLocation = newDataPage.insertRecord(record);
		newPageInfo.recct++;
		newPageInfo.availspace = newDataPage.available_space();

		// add the DataPageInfo to directory
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);

		PageId nextDirPageId = new PageId(0);
		HFPage currentDirPage = new HFPage();
		boolean foundSpaceInDirectoryPage = false;
		while (currentDirPageId.pid != INVALID_PAGE) {
			// HashUtils.log("checking dir page id :"+currentDirPageId.pid);
			pinPage(currentDirPageId, currentDirPage, false);

			if (currentDirPage.available_space() >= newPageInfo.size) {
				foundSpaceInDirectoryPage = true;

				byte[] tmpData = newPageInfo.convertToTuple().getTupleByteArray();
				RID newPageLocationInExistingDirectoryPage = currentDirPage.insertRecord(tmpData);
				// HashUtils.log("newPageLocationInExistingDirectoryPage
				// "+newPageLocationInExistingDirectoryPage);
				RID tmprid = currentDirPage.firstRecord();

			}

			if (foundSpaceInDirectoryPage == true) {
				unpinPage(currentDirPageId, true /* DIRTY */);
				break;
			}

			nextDirPageId = currentDirPage.getNextPage();

			if (nextDirPageId.pid == INVALID_PAGE) { // insert another directory page
				HashUtils.log("Inserting another directory page");
				Page pageinbuffer = new Page();
				nextDirPageId = newPage(pageinbuffer, 1);
				// need check error!
				if (nextDirPageId == null)
					throw new HFException(null, "can't new pae");

				HFPage newDirPage = new HFPage();
				// initialize new directory page
				newDirPage.init(nextDirPageId, pageinbuffer);
				PageId temppid = new PageId(INVALID_PAGE);
				newDirPage.setNextPage(temppid);
				newDirPage.setPrevPage(currentDirPageId);

				// update current directory page and unpin it
				currentDirPage.setNextPage(nextDirPageId);
				unpinPage(currentDirPageId, true/* dirty */);

				// insert the new page info in the new directory page
				byte[] tmpData = newPageInfo.convertToTuple().getTupleByteArray();
				RID newPageLocationInNewDirectoryPage = newDirPage.insertRecord(tmpData);
				// HashUtils.log("newPageLocationInNewDirectoryPage
				// "+newPageLocationInNewDirectoryPage);

				unpinPage(nextDirPageId, true/* dirty */);
				break;

			}
			unpinPage(currentDirPageId, false /* undirty */);

			currentDirPageId.pid = nextDirPageId.pid;
		}

		// cleanup
		unpinPage(newPageInfo.pageId, true /* = DIRTY */);

		return insertedRecordLocation;

	}

	@Override
	public RID insertRecord(byte[] recPtr) throws InvalidSlotNumberException, InvalidTupleSizeException,
			SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException, IOException {
		throw new IllegalArgumentException("Funtion not supported for clustered file");
	}

	/**
	 * deleted all record on page which match the supplied record<br>
	 * return list of RIDs on data file for which the records were deleted and if data page was deleted
	 * @param record the record to be deleted
	 * @param pageId page from which records whould be deleted
	 */
	public Pair<List<RID>, Boolean> deleteRecordFromPage(byte[] record, PageId pageId) throws Exception {
		HashUtils.Pair<List<RID>, Boolean> toBeReturned = new Pair<>();
		toBeReturned.second = false;
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);

		PageId nextDirPageId = new PageId(0);

		HFPage currentDirPage = new HFPage();
		RID ridOfPageInDirPage = null;
		boolean found = false;
		// 1. find the directory page which has page info of pageId
		while (currentDirPageId.pid != INVALID_PAGE) {
			HashUtils.log("checking dir page id :" + currentDirPageId.pid);
			pinPage(currentDirPageId, currentDirPage, false);

			RID rid = new RID();
			Tuple atuple;
			for (rid = currentDirPage.firstRecord(); rid != null; // rid==NULL means no more record
					rid = currentDirPage.nextRecord(rid)) {
				atuple = currentDirPage.getRecord(rid);
				DataPageInfo dpinfo = new DataPageInfo(atuple);

				if (dpinfo.pageId.pid == pageId.pid) {
					HashUtils.log("page id " + pageId + " found in directory page " + currentDirPageId);
					found = true;
					ridOfPageInDirPage = new RID(new PageId(rid.pageNo.pid), rid.slotNo);
					break;
				}

			}

			unpinPage(currentDirPageId, false /* undirty */);
			if (found == true) {
				// HashUtils.log("breaking from outer loop");
				break;
			}
			nextDirPageId = currentDirPage.getNextPage();
			currentDirPageId.pid = nextDirPageId.pid;
		}

		pinPage(currentDirPageId, currentDirPage, false);
		DataPageInfo pdpinfo = new DataPageInfo(currentDirPage.returnRecord(ridOfPageInDirPage));
		//2. Find RIDs in the data page which are to be deleted by mactching each record with supplied record
		List<RID> ridInPageToDelete = new ArrayList<>();
		HFPage currentDataPage = new HFPage();
		pinPage(pageId, currentDataPage, false);
		RID ptr = currentDataPage.firstRecord();
		while (ptr != null) {
			byte[] tup = currentDataPage.getRecord(ptr).getTupleByteArray();
			if (Arrays.equals(tup, record)) {
				ridInPageToDelete.add(new RID(new PageId(ptr.pageNo.pid), ptr.slotNo));
			}
			ptr = currentDataPage.nextRecord(ptr);
		}
		//3. Delete all the records on the page
		for (RID ridddd : ridInPageToDelete) {
			currentDataPage.deleteRecord(ridddd);
		}
		
		pdpinfo.recct -= ridInPageToDelete.size();
		//4. code copied from heapfile.delete method, manage the directory page
		if (pdpinfo.recct >= 1) {
			// more records remain on datapage so it still hangs around.
			// we just need to modify its directory entry
			HashUtils.log("Not deleting data page info in directory page");

			pdpinfo.availspace = currentDataPage.available_space();
			pdpinfo.flushToTuple();
			unpinPage(pageId, true /* = DIRTY */);

			unpinPage(currentDirPageId, true /* = DIRTY */);

		} else {
			// the record is already deleted:
			// we're removing the last record on datapage so free datapage
			// also, free the directory page if
			// a) it's not the first directory page, and
			// b) we've removed the last DataPageInfo record on it.

			// delete empty datapage: (does it get unpinned automatically? -NO, Ranjani)
			unpinPage(pageId, false /* undirty */);

			freePage(pageId);
			toBeReturned.second = true;
			// delete corresponding DataPageInfo-entry on the directory page:
			// currentDataPageRid points to datapage (from for loop above)

			currentDirPage.deleteRecord(ridOfPageInDirPage);

			// ASSERTIONS:
			// - currentDataPage, currentDataPageId invalid
			// - empty datapage unpinned and deleted

			// now check whether the directory page is empty:

			ridOfPageInDirPage = currentDirPage.firstRecord();

			// st == OK: we still found a datapageinfo record on this directory page
			PageId pageId1;
			pageId1 = currentDirPage.getPrevPage();
			if ((ridOfPageInDirPage == null) && (pageId1.pid != INVALID_PAGE)) {
				// the directory-page is not the first directory page and it is empty:
				// delete it

				// point previous page around deleted page:

				HFPage prevDirPage = new HFPage();
				pinPage(pageId1, prevDirPage, false);

				pageId1 = currentDirPage.getNextPage();
				prevDirPage.setNextPage(pageId1);
				pageId1 = currentDirPage.getPrevPage();
				unpinPage(pageId1, true /* = DIRTY */);

				// set prevPage-pointer of next Page
				pageId1 = currentDirPage.getNextPage();
				if (pageId1.pid != INVALID_PAGE) {
					HFPage nextDirPage = new HFPage();
					pageId1 = currentDirPage.getNextPage();
					pinPage(pageId1, nextDirPage, false);

					// nextDirPage.openHFpage(apage);

					pageId1 = currentDirPage.getPrevPage();
					nextDirPage.setPrevPage(pageId1);
					pageId1 = currentDirPage.getNextPage();
					unpinPage(pageId1, true /* = DIRTY */);

				}

				// delete empty directory page: (automatically unpinned?)
				unpinPage(currentDirPageId, false/* undirty */);
				freePage(currentDirPageId);

			} else {
				// either (the directory page has at least one more datapagerecord
				// entry) or (it is the first directory page):
				// in both cases we do not delete it, but we have to unpin it:

				unpinPage(currentDirPageId, true /* == DIRTY */);

			}
		}
		toBeReturned.first = ridInPageToDelete;
		return toBeReturned;
	}

	@Override
	public boolean deleteRecord(RID rid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
			HFBufMgrException, HFDiskMgrException, Exception {
		throw new IllegalArgumentException("Funtion not supported for clustered file");
	}

	public void printToConsole(Function<byte[], String> mapper) throws Exception {
		Scan scan = openScan();
		RID rid = new RID();
		Tuple tup;
		boolean done = false;
		while (!done) {
			tup = scan.getNext(rid);

			if (tup == null) {
				done = true;
				break;
			}
			System.out.println("Tuple " + mapper.apply(tup.getTupleByteArray()) + " @ " + rid);

		}
		scan.closescan();
	}

}
