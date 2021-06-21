package heap;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import btree.BT;
import btree.BTFileScan;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.IndexFileScan;
import btree.InsertRecException;
import btree.IntegerKey;
import btree.IteratorException;
import btree.KeyClass;
import btree.KeyDataEntry;
import btree.KeyNotMatchException;
import btree.LeafData;
import btree.PinPageException;
import btree.ScanIteratorException;
import btree.StringKey;
import btree.UnpinPageException;
import diskmgr.*;
import bufmgr.*;
import clustered_btree.ClusteredBTSortedPage;
import clustered_btree.ClusteredBTreeFile;
import global.*;
import index.IndexScan;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.TupleUtils;

/**  This heapfile implementation is directory-based. We maintain a
 *  directory of info about the data pages (which are of type ClusteredBTSortedPage
 *  when loaded into memory).  The directory itself is also composed
 *  of HFPages, with each record being of type DataPageInfo
 *  as defined below.
 *
 *  The first directory page is a header page for the entire database
 *  (it is the one to which our filename is mapped by the DB).
 *  All directory pages are in a doubly-linked list of pages, each
 *  directory entry points to a single data page, which contains
 *  the actual records.
 *
 *  The heapfile data pages are implemented as slotted pages, with
 *  the slots at the front and the records in the back, both growing
 *  into the free space in the middle of the page.
 *
 *  We can store roughly pagesize/sizeof(DataPageInfo) records per
 *  directory page; for any given HeapFile insertion, it is likely
 *  that at least one of those referenced data pages will have
 *  enough free space to satisfy the request.
 */


/** DataPageInfo class : the type of records stored on a directory page.
 *
 * April 9, 1998
 */

public class ClusteredHeapfile extends Heapfile implements GlobalConst {  

	/* get a new datapage from the buffer manager and initialize dpinfo
     @param dpinfop the information in the new ClusteredBTSortedPage
	 */
	protected ClusteredBTSortedPage _newDatapage(DataPageInfo dpinfop)
			throws HFException,
			HFBufMgrException,
			HFDiskMgrException,
			IOException
	{
		Page apage = new Page();
		PageId pageId = new PageId();
		pageId = newPage(apage, 1);

		if(pageId == null)
			throw new HFException(null, "can't new pae");

		// initialize internal values of the new page:

		ClusteredBTSortedPage hfpage = new ClusteredBTSortedPage();
		hfpage.init(pageId, apage);

		dpinfop.pageId.pid = pageId.pid;
		dpinfop.recct = 0;
		dpinfop.availspace = hfpage.available_space();

		return hfpage;

	} // end of _newDatapage

	/* Internal HeapFile function (used in getRecord and updateRecord):
     returns pinned directory page and pinned data page of the specified 
     user record(rid) and true if record is found.
     If the user record cannot be found, return false.
	 */
	private boolean  _findDataPage( RID rid,
			PageId dirPageId, ClusteredBTSortedPage dirpage,
			PageId dataPageId, ClusteredBTSortedPage datapage,
			RID rpDataPageRid) 
					throws InvalidSlotNumberException, 
					InvalidTupleSizeException, 
					HFException,
					HFBufMgrException,
					HFDiskMgrException,
					Exception
	{
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);

		ClusteredBTSortedPage currentDirPage = new ClusteredBTSortedPage();
		ClusteredBTSortedPage currentDataPage = new ClusteredBTSortedPage();
		RID currentDataPageRid = new RID();
		PageId nextDirPageId = new PageId();
		// datapageId is stored in dpinfo.pageId 


		pinPage(currentDirPageId, currentDirPage, false/*read disk*/);

		Tuple atuple = new Tuple();

		while (currentDirPageId.pid != INVALID_PAGE)
		{// Start While01
			// ASSERTIONS:
			//  currentDirPage, currentDirPageId valid and pinned and Locked.

			for( currentDataPageRid = currentDirPage.firstRecord();
					currentDataPageRid != null;
					currentDataPageRid = currentDirPage.nextRecord(currentDataPageRid))
			{
				try{
					atuple = currentDirPage.getRecord(currentDataPageRid);
				}
				catch (InvalidSlotNumberException e)// check error! return false(done) 
				{
					return false;
				}

				DataPageInfo dpinfo = new DataPageInfo(atuple);
				try{
					pinPage(dpinfo.pageId, currentDataPage, false/*Rddisk*/);


					//check error;need unpin currentDirPage
				}catch (Exception e)
				{
					unpinPage(currentDirPageId, false/*undirty*/);
					dirpage = null;
					datapage = null;
					throw e;
				}



				// ASSERTIONS:
				// - currentDataPage, currentDataPageRid, dpinfo valid
				// - currentDataPage pinned

				if(dpinfo.pageId.pid==rid.pageNo.pid)
				{
					atuple = currentDataPage.returnRecord(rid);
					// found user's record on the current datapage which itself
					// is indexed on the current dirpage.  Return both of these.

					dirpage.setpage(currentDirPage.getpage());
					dirPageId.pid = currentDirPageId.pid;

					datapage.setpage(currentDataPage.getpage());
					dataPageId.pid = dpinfo.pageId.pid;

					rpDataPageRid.pageNo.pid = currentDataPageRid.pageNo.pid;
					rpDataPageRid.slotNo = currentDataPageRid.slotNo;
					return true;
				}
				else
				{
					// user record not found on this datapage; unpin it
					// and try the next one
					unpinPage(dpinfo.pageId, false /*undirty*/);

				}

			}

			// if we would have found the correct datapage on the current
			// directory page we would have already returned.
			// therefore:
			// read in next directory page:

			nextDirPageId = currentDirPage.getNextPage();
			try{
				unpinPage(currentDirPageId, false /*undirty*/);
			}
			catch(Exception e) {
				throw new HFException (e, "heapfile,_find,unpinpage failed");
			}

			currentDirPageId.pid = nextDirPageId.pid;
			if(currentDirPageId.pid != INVALID_PAGE)
			{
				pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);
				if(currentDirPage == null)
					throw new HFException(null, "pinPage return null page");  
			}


		} // end of While01
		// checked all dir pages and all data pages; user record not found:(

		dirPageId.pid = dataPageId.pid = INVALID_PAGE;

		return false;   


	} // end of _findDatapage		     

	/** Initialize.  A null name produces a temporary heapfile which will be
	 * deleted by the destructor.  If the name already denotes a file, the
	 * file is opened; otherwise, a new empty file is created.
	 *
	 * @exception HFException heapfile exception
	 * @exception HFBufMgrException exception thrown from bufmgr layer
	 * @exception HFDiskMgrException exception thrown from diskmgr layer
	 * @exception IOException I/O errors
	 */
	public  ClusteredHeapfile(String name) 
			throws HFException, 
			HFBufMgrException,
			HFDiskMgrException,
			IOException

	{
		super(name);  

	} // end of constructor 

	/** Return number of records in file.
	 *
	 * @exception InvalidSlotNumberException invalid slot number
	 * @exception InvalidTupleSizeException invalid tuple size
	 * @exception HFBufMgrException exception thrown from bufmgr layer
	 * @exception HFDiskMgrException exception thrown from diskmgr layer
	 * @exception IOException I/O errors
	 */
	public int getRecCnt() 
			throws InvalidSlotNumberException, 
			InvalidTupleSizeException, 
			HFDiskMgrException,
			HFBufMgrException,
			IOException

	{
		int answer = 0;
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);

		PageId nextDirPageId = new PageId(0);

		ClusteredBTSortedPage currentDirPage = new ClusteredBTSortedPage();
		Page pageinbuffer = new Page();

		while(currentDirPageId.pid != INVALID_PAGE)
		{
			pinPage(currentDirPageId, currentDirPage, false);

			RID rid = new RID();
			Tuple atuple;
			for (rid = currentDirPage.firstRecord();
					rid != null;	// rid==NULL means no more record
					rid = currentDirPage.nextRecord(rid))
			{
				atuple = currentDirPage.getRecord(rid);
				DataPageInfo dpinfo = new DataPageInfo(atuple);

				answer += dpinfo.recct;
			}

			// ASSERTIONS: no more record
			// - we have read all datapage records on
			//   the current directory page.

			nextDirPageId = currentDirPage.getNextPage();
			unpinPage(currentDirPageId, false /*undirty*/);
			currentDirPageId.pid = nextDirPageId.pid;
		}

		// ASSERTIONS:
		// - if error, exceptions
		// - if end of heapfile reached: currentDirPageId == INVALID_PAGE
		// - if not yet end of heapfile: currentDirPageId valid


		return answer;
	} // end of getRecCnt

	/** Insert record into file, return its Rid.
	 *
	 * @param recPtr pointer of the record
	 * @param recLen the length of the record
	 *
	 * @exception InvalidSlotNumberException invalid slot number
	 * @exception InvalidTupleSizeException invalid tuple size
	 * @exception SpaceNotAvailableException no space left
	 * @exception HFException heapfile exception
	 * @exception HFBufMgrException exception thrown from bufmgr layer
	 * @exception HFDiskMgrException exception thrown from diskmgr layer
	 * @exception IOException I/O errors
	 *
	 * @return the rid of the record
	 * @throws InsertRecException 
	 */
	public RID insertRecord(byte[] recPtr, AttrType[] attrtype, short[] strsizes) 
			throws InvalidSlotNumberException,  
			InvalidTupleSizeException,
			SpaceNotAvailableException,
			HFException,
			HFBufMgrException,
			HFDiskMgrException,
			IOException, InsertRecException
	{
		int dpinfoLen = 0;	
		int recLen = recPtr.length;
		boolean found;
		RID currentDataPageRid = new RID();
		Page pageinbuffer = new Page();
		ClusteredBTSortedPage currentDirPage = new ClusteredBTSortedPage();
		ClusteredBTSortedPage currentDataPage = new ClusteredBTSortedPage();

		ClusteredBTSortedPage nextDirPage = new ClusteredBTSortedPage(); 
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);
		PageId nextDirPageId = new PageId();  // OK

		pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);

		found = false;
		Tuple atuple;
		DataPageInfo dpinfo = new DataPageInfo();
		while (found == false)
		{ //Start While01
			// look for suitable dpinfo-struct
			for (currentDataPageRid = currentDirPage.firstRecord();
					currentDataPageRid != null;
					currentDataPageRid = 
							currentDirPage.nextRecord(currentDataPageRid))
			{
				atuple = currentDirPage.getRecord(currentDataPageRid);

				dpinfo = new DataPageInfo(atuple);

				// need check the record length == DataPageInfo'slength

				if(recLen <= dpinfo.availspace)
				{
					found = true;
					break;
				}  
			}

			// two cases:
			// (1) found == true:
			//     currentDirPage has a datapagerecord which can accomodate
			//     the record which we have to insert
			// (2) found == false:
			//     there is no datapagerecord on the current directory page
			//     whose corresponding datapage has enough space free
			//     several subcases: see below
			if(found == false)
			{ //Start IF01
				// case (2)

				//System.out.println("no datapagerecord on the current directory is OK");
				//System.out.println("dirpage availspace "+currentDirPage.available_space());

				// on the current directory page is no datapagerecord which has
				// enough free space
				//
				// two cases:
				//
				// - (2.1) (currentDirPage->available_space() >= sizeof(DataPageInfo):
				//         if there is enough space on the current directory page
				//         to accomodate a new datapagerecord (type DataPageInfo),
				//         then insert a new DataPageInfo on the current directory
				//         page
				// - (2.2) (currentDirPage->available_space() <= sizeof(DataPageInfo):
				//         look at the next directory page, if necessary, create it.

				if(currentDirPage.available_space() >= dpinfo.size)
				{ 
					//Start IF02
					// case (2.1) : add a new data page record into the
					//              current directory page
					currentDataPage = _newDatapage(dpinfo); 
					// currentDataPage is pinned! and dpinfo->pageId is also locked
					// in the exclusive mode  

					// didn't check if currentDataPage==NULL, auto exception


					// currentDataPage is pinned: insert its record
					// calling a ClusteredBTSortedPage function



					atuple = dpinfo.convertToTuple();

					byte [] tmpData = atuple.getTupleByteArray();
					currentDataPageRid = currentDirPage.insertRecord(tmpData);

					RID tmprid = currentDirPage.firstRecord();


					// need catch error here!
					if(currentDataPageRid == null)
						throw new HFException(null, "no space to insert rec.");  

					// end the loop, because a new datapage with its record
					// in the current directorypage was created and inserted into
					// the heapfile; the new datapage has enough space for the
					// record which the user wants to insert

					found = true;

				} //end of IF02
				else
				{  //Start else 02
					// case (2.2)
					nextDirPageId = currentDirPage.getNextPage();
					// two sub-cases:
					//
					// (2.2.1) nextDirPageId != INVALID_PAGE:
					//         get the next directory page from the buffer manager
					//         and do another look
					// (2.2.2) nextDirPageId == INVALID_PAGE:
					//         append a new directory page at the end of the current
					//         page and then do another loop

					if (nextDirPageId.pid != INVALID_PAGE) 
					{ //Start IF03
						// case (2.2.1): there is another directory page:
						unpinPage(currentDirPageId, false);

						currentDirPageId.pid = nextDirPageId.pid;

						pinPage(currentDirPageId,
								currentDirPage, false);



						// now go back to the beginning of the outer while-loop and
						// search on the current directory page for a suitable datapage
					} //End of IF03
					else
					{  //Start Else03
						// case (2.2): append a new directory page after currentDirPage
						//             since it is the last directory page
						nextDirPageId = newPage(pageinbuffer, 1);
						// need check error!
						if(nextDirPageId == null)
							throw new HFException(null, "can't new pae");

						// initialize new directory page
						nextDirPage.init(nextDirPageId, pageinbuffer);
						PageId temppid = new PageId(INVALID_PAGE);
						nextDirPage.setNextPage(temppid);
						nextDirPage.setPrevPage(currentDirPageId);

						// update current directory page and unpin it
						// currentDirPage is already locked in the Exclusive mode
						currentDirPage.setNextPage(nextDirPageId);
						unpinPage(currentDirPageId, true/*dirty*/);

						currentDirPageId.pid = nextDirPageId.pid;
						currentDirPage = new ClusteredBTSortedPage(nextDirPage);

						// remark that MINIBASE_BM->newPage already
						// pinned the new directory page!
						// Now back to the beginning of the while-loop, using the
						// newly created directory page.

					} //End of else03
				} // End of else02
				// ASSERTIONS:
				// - if found == true: search will end and see assertions below
				// - if found == false: currentDirPage, currentDirPageId
				//   valid and pinned

			}//end IF01
			else
			{ //Start else01
				// found == true:
				// we have found a datapage with enough space,
				// but we have not yet pinned the datapage:

				// ASSERTIONS:
				// - dpinfo valid

				// System.out.println("find the dirpagerecord on current page");

				pinPage(dpinfo.pageId, currentDataPage, false);
				//currentDataPage.openHFpage(pageinbuffer);


			}//End else01
		} //end of While01

		// ASSERTIONS:
		// - currentDirPageId, currentDirPage valid and pinned
		// - dpinfo.pageId, currentDataPageRid valid
		// - currentDataPage is pinned!

		if ((dpinfo.pageId).pid == INVALID_PAGE) // check error!
			throw new HFException(null, "invalid PageId");

		if (!(currentDataPage.available_space() >= recLen))
			throw new SpaceNotAvailableException(null, "no available space");

		if (currentDataPage == null)
			throw new HFException(null, "can't find Data page");


		RID rid;
		rid = currentDataPage.insertRecord(recPtr, attrtype, strsizes, 1);

		dpinfo.recct++;
		dpinfo.availspace = currentDataPage.available_space();


		unpinPage(dpinfo.pageId, true /* = DIRTY */);

		// DataPage is now released
		atuple = currentDirPage.returnRecord(currentDataPageRid);
		DataPageInfo dpinfo_ondirpage = new DataPageInfo(atuple);


		dpinfo_ondirpage.availspace = dpinfo.availspace;
		dpinfo_ondirpage.recct = dpinfo.recct;
		dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;
		dpinfo_ondirpage.flushToTuple();


		unpinPage(currentDirPageId, true /* = DIRTY */);


		return rid;

	}

	/** Delete record from file with given rid.
	 *
	 * @exception InvalidSlotNumberException invalid slot number
	 * @exception InvalidTupleSizeException invalid tuple size
	 * @exception HFException heapfile exception
	 * @exception HFBufMgrException exception thrown from bufmgr layer
	 * @exception HFDiskMgrException exception thrown from diskmgr layer
	 * @exception Exception other exception
	 *
	 * @return true record deleted  false:record not found
	 */
	public boolean deleteRecord(RID rid)  
			throws InvalidSlotNumberException, 
			InvalidTupleSizeException, 
			HFException, 
			HFBufMgrException,
			HFDiskMgrException,
			Exception

	{
		boolean status;
		ClusteredBTSortedPage currentDirPage = new ClusteredBTSortedPage();
		PageId currentDirPageId = new PageId();
		ClusteredBTSortedPage currentDataPage = new ClusteredBTSortedPage();
		PageId currentDataPageId = new PageId();
		RID currentDataPageRid = new RID();

		status = _findDataPage(rid,
				currentDirPageId, currentDirPage, 
				currentDataPageId, currentDataPage,
				currentDataPageRid);

		if(status != true) return status;	// record not found

		// ASSERTIONS:
		// - currentDirPage, currentDirPageId valid and pinned
		// - currentDataPage, currentDataPageid valid and pinned

		// get datapageinfo from the current directory page:
		Tuple atuple;	

		atuple = currentDirPage.returnRecord(currentDataPageRid);
		DataPageInfo pdpinfo = new DataPageInfo(atuple);

		// delete the record on the datapage
		currentDataPage.deleteRecord(rid);

		pdpinfo.recct--;
		pdpinfo.flushToTuple();	//Write to the buffer pool
		if (pdpinfo.recct >= 1) 
		{
			// more records remain on datapage so it still hangs around.  
			// we just need to modify its directory entry

			pdpinfo.availspace = currentDataPage.available_space();
			pdpinfo.flushToTuple();
			unpinPage(currentDataPageId, true /* = DIRTY*/);

			unpinPage(currentDirPageId, true /* = DIRTY */);


		}
		else
		{
			// the record is already deleted:
			// we're removing the last record on datapage so free datapage
			// also, free the directory page if 
			//   a) it's not the first directory page, and 
			//   b) we've removed the last DataPageInfo record on it.

			// delete empty datapage: (does it get unpinned automatically? -NO, Ranjani)
			unpinPage(currentDataPageId, false /*undirty*/);

			freePage(currentDataPageId);

			// delete corresponding DataPageInfo-entry on the directory page:
			// currentDataPageRid points to datapage (from for loop above)

			currentDirPage.deleteRecord(currentDataPageRid);


			// ASSERTIONS:
			// - currentDataPage, currentDataPageId invalid
			// - empty datapage unpinned and deleted

			// now check whether the directory page is empty:

			currentDataPageRid = currentDirPage.firstRecord();

			// st == OK: we still found a datapageinfo record on this directory page
			PageId pageId;
			pageId = currentDirPage.getPrevPage();
			if((currentDataPageRid == null)&&(pageId.pid != INVALID_PAGE))
			{
				// the directory-page is not the first directory page and it is empty:
				// delete it

				// point previous page around deleted page:

				ClusteredBTSortedPage prevDirPage = new ClusteredBTSortedPage();
				pinPage(pageId, prevDirPage, false);

				pageId = currentDirPage.getNextPage();
				prevDirPage.setNextPage(pageId);
				pageId = currentDirPage.getPrevPage();
				unpinPage(pageId, true /* = DIRTY */);


				// set prevPage-pointer of next Page
				pageId = currentDirPage.getNextPage();
				if(pageId.pid != INVALID_PAGE)
				{
					ClusteredBTSortedPage nextDirPage = new ClusteredBTSortedPage();
					pageId = currentDirPage.getNextPage();
					pinPage(pageId, nextDirPage, false);

					//nextDirPage.openHFpage(apage);

					pageId = currentDirPage.getPrevPage();
					nextDirPage.setPrevPage(pageId);
					pageId = currentDirPage.getNextPage();
					unpinPage(pageId, true /* = DIRTY */);

				}

				// delete empty directory page: (automatically unpinned?)
				unpinPage(currentDirPageId, false/*undirty*/);
				freePage(currentDirPageId);


			}
			else
			{
				// either (the directory page has at least one more datapagerecord
				// entry) or (it is the first directory page):
				// in both cases we do not delete it, but we have to unpin it:

				unpinPage(currentDirPageId, true /* == DIRTY */);


			}
		}
		return true;
	}


	/** Updates the specified record in the heapfile.
	 * @param rid: the record which needs update
	 * @param newtuple: the new content of the record
	 *
	 * @exception InvalidSlotNumberException invalid slot number
	 * @exception InvalidUpdateException invalid update on record
	 * @exception InvalidTupleSizeException invalid tuple size
	 * @exception HFException heapfile exception
	 * @exception HFBufMgrException exception thrown from bufmgr layer
	 * @exception HFDiskMgrException exception thrown from diskmgr layer
	 * @exception Exception other exception
	 * @return ture:update success   false: can't find the record
	 */
	public boolean updateRecord(RID rid, Tuple newtuple) 
			throws InvalidSlotNumberException, 
			InvalidUpdateException, 
			InvalidTupleSizeException,
			HFException, 
			HFDiskMgrException,
			HFBufMgrException,
			Exception
	{
		boolean status;
		ClusteredBTSortedPage dirPage = new ClusteredBTSortedPage();
		PageId currentDirPageId = new PageId();
		ClusteredBTSortedPage dataPage = new ClusteredBTSortedPage();
		PageId currentDataPageId = new PageId();
		RID currentDataPageRid = new RID();

		status = _findDataPage(rid,
				currentDirPageId, dirPage, 
				currentDataPageId, dataPage,
				currentDataPageRid);

		if(status != true) return status;	// record not found
		Tuple atuple = new Tuple();
		atuple = dataPage.returnRecord(rid);

		// Assume update a record with a record whose length is equal to
		// the original record

		if(newtuple.getLength() != atuple.getLength())
		{
			unpinPage(currentDataPageId, false /*undirty*/);
			unpinPage(currentDirPageId, false /*undirty*/);

			throw new InvalidUpdateException(null, "invalid record update");

		}

		// new copy of this record fits in old space;
		atuple.tupleCopy(newtuple);
		unpinPage(currentDataPageId, true /* = DIRTY */);

		unpinPage(currentDirPageId, false /*undirty*/);


		return true;
	}


	/** Read record from file, returning pointer and length.
	 * @param rid Record ID
	 *
	 * @exception InvalidSlotNumberException invalid slot number
	 * @exception InvalidTupleSizeException invalid tuple size
	 * @exception SpaceNotAvailableException no space left
	 * @exception HFException heapfile exception
	 * @exception HFBufMgrException exception thrown from bufmgr layer
	 * @exception HFDiskMgrException exception thrown from diskmgr layer
	 * @exception Exception other exception
	 *
	 * @return a Tuple. if Tuple==null, no more tuple
	 */
	public  Tuple getRecord(RID rid) 
			throws InvalidSlotNumberException, 
			InvalidTupleSizeException, 
			HFException, 
			HFDiskMgrException,
			HFBufMgrException,
			Exception
	{
		boolean status;
		ClusteredBTSortedPage dirPage = new ClusteredBTSortedPage();
		PageId currentDirPageId = new PageId();
		ClusteredBTSortedPage dataPage = new ClusteredBTSortedPage();
		PageId currentDataPageId = new PageId();
		RID currentDataPageRid = new RID();

		status = _findDataPage(rid,
				currentDirPageId, dirPage, 
				currentDataPageId, dataPage,
				currentDataPageRid);

		if(status != true) return null; // record not found 

		Tuple atuple = new Tuple();
		atuple = dataPage.getRecord(rid);

		/*
		 * getRecord has copied the contents of rid into recPtr and fixed up
		 * recLen also.  We simply have to unpin dirpage and datapage which
		 * were originally pinned by _findDataPage.
		 */    

		unpinPage(currentDataPageId,false /*undirty*/);

		unpinPage(currentDirPageId,false /*undirty*/);


		return  atuple;  //(true?)OK, but the caller need check if atuple==NULL

	}


	/** Initiate a sequential scan.
	 * @exception InvalidTupleSizeException Invalid tuple size
	 * @exception IOException I/O errors
	 *
	 */
	public Scan openScan() 
			throws InvalidTupleSizeException,
			IOException
	{
		Scan newscan = new Scan(this);
		return newscan;
	}


	/** Delete the file from the database.
	 *
	 * @exception InvalidSlotNumberException invalid slot number
	 * @exception InvalidTupleSizeException invalid tuple size
	 * @exception FileAlreadyDeletedException file is deleted already
	 * @exception HFBufMgrException exception thrown from bufmgr layer
	 * @exception HFDiskMgrException exception thrown from diskmgr layer
	 * @exception IOException I/O errors
	 */
	public void deleteFile()  
			throws InvalidSlotNumberException, 
			FileAlreadyDeletedException, 
			InvalidTupleSizeException, 
			HFBufMgrException,
			HFDiskMgrException,
			IOException
	{
		if(_file_deleted ) 
			throw new FileAlreadyDeletedException(null, "file alread deleted");


		// Mark the deleted flag (even if it doesn't get all the way done).
		_file_deleted = true;

		// Deallocate all data pages
		PageId currentDirPageId = new PageId();
		currentDirPageId.pid = _firstDirPageId.pid;
		PageId nextDirPageId = new PageId();
		nextDirPageId.pid = 0;
		Page pageinbuffer = new Page();
		ClusteredBTSortedPage currentDirPage =  new ClusteredBTSortedPage();
		Tuple atuple;

		pinPage(currentDirPageId, currentDirPage, false);
		//currentDirPage.openHFpage(pageinbuffer);

		RID rid = new RID();
		while(currentDirPageId.pid != INVALID_PAGE)
		{      
			for(rid = currentDirPage.firstRecord();
					rid != null;
					rid = currentDirPage.nextRecord(rid))
			{
				atuple = currentDirPage.getRecord(rid);
				DataPageInfo dpinfo = new DataPageInfo( atuple);
				//int dpinfoLen = arecord.length;

				freePage(dpinfo.pageId);

			}
			// ASSERTIONS:
			// - we have freePage()'d all data pages referenced by
			// the current directory page.

			nextDirPageId = currentDirPage.getNextPage();
			unpinPage(currentDirPageId, true);
			freePage(currentDirPageId);

			currentDirPageId.pid = nextDirPageId.pid;
			if (nextDirPageId.pid != INVALID_PAGE) 
			{
				pinPage(currentDirPageId, currentDirPage, false);
				//currentDirPage.openHFpage(pageinbuffer);
			}
		}

		delete_file_entry( _fileName );
	}

	private PageId get_file_entry(String filename)
			throws HFDiskMgrException {

		PageId tmpId = new PageId();

		try {
			tmpId = SystemDefs.JavabaseDB.get_file_entry(filename);
		}
		catch (Exception e) {
			throw new HFDiskMgrException(e,"Heapfile.java: get_file_entry() failed");
		}

		return tmpId;

	} // end of get_file_entry

	private void add_file_entry(String filename, PageId pageno)
			throws HFDiskMgrException {

		try {
			SystemDefs.JavabaseDB.add_file_entry(filename,pageno);
		}
		catch (Exception e) {
			throw new HFDiskMgrException(e,"Heapfile.java: add_file_entry() failed");
		}

	} // end of add_file_entry

	private void delete_file_entry(String filename)
			throws HFDiskMgrException {

		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		}
		catch (Exception e) {
			throw new HFDiskMgrException(e,"Heapfile.java: delete_file_entry() failed");
		}

	} // end of delete_file_entry

	/** Insert record into file in sorted manner, return its Rid.
	 *
	 * @param recPtr pointer of the record
	 * @param recLen the length of the record
	 * @param attrType array of attrtypes of the tuple
	 * @param strsizes array of the str sizes of the tuple
	 * @param key_index field number of the key attribute in the attrtype array
	 *
	 * @return the rid of the record
	 * @throws Exception 
	 */
	public RID insertRecord(Tuple insert_tuple, AttrType[] attrType, short[] strsizes, int key_index, String btsfilename) 
			throws Exception
	{
//		print_empty_slot();
//		insert_tuple.print(attrType);
		if ( btsfilename == null ) {
			insertRecord(insert_tuple.getTupleByteArray());
		}
		//System.out.println("Into te new insert method");
		//t1.print(attrType);
		RID currentDataPageRid = new RID();
		RID data_inserted_rid = new RID();

		ClusteredBTSortedPage currentDirPage = new ClusteredBTSortedPage();
		ClusteredBTSortedPage currentDataPage = new ClusteredBTSortedPage();
		ClusteredBTSortedPage nextDirPage = new ClusteredBTSortedPage();
		ClusteredBTSortedPage LastBTPage = new ClusteredBTSortedPage();
		ClusteredBTSortedPage pageinbuffer = new ClusteredBTSortedPage();
		ClusteredBTSortedPage newDataPage = new ClusteredBTSortedPage();

		PageId currentDirPageId = new PageId(_firstDirPageId.pid);
		PageId lastBTPageId = new PageId();
		PageId nextDirPageId = new PageId();

		pinPage( currentDirPageId, currentDirPage, false );

		/* enter data in the end of the file atleast */
		KeyClass key = null;
		KeyDataEntry nextentry = null;
		KeyDataEntry preventry = null;

		key = TupleUtils.get_key_from_tuple_attrtype(insert_tuple, attrType[key_index-1], key_index);
		/*if ( attrType[key_index-1].attrType == AttrType.attrInteger )
		 key = new IntegerKey(insert_tuple.getIntFld(key_index));
	 else
		 key = new StringKey(insert_tuple.getStrFld(key_index));
		 */
		ClusteredBTreeFile btf  = new ClusteredBTreeFile(btsfilename);
		BTFileScan indScan = ((ClusteredBTreeFile)btf).new_scan(key, null);
		nextentry = indScan.get_next();


		boolean found = false;
		DataPageInfo dpinfo = new DataPageInfo();
		DataPageInfo newdpinfo = new DataPageInfo();
		Tuple atuple;
		if ( nextentry == null ) 
		{ //this key is going to be the highest key in the btree
			//Lookup the last datapage and see if there is available space
			indScan = ((ClusteredBTreeFile)btf).new_scan(null, null);
			nextentry = indScan.get_next();
			while ( nextentry != null ) {
				preventry = new KeyDataEntry( nextentry.key, nextentry.data );
				nextentry = indScan.get_next();
			}
			RID rid_nextentry = ((LeafData)preventry.data).getData();

			lastBTPageId = new PageId(rid_nextentry.pageNo.pid);

			ClusteredBTSortedPage lookup_dirPage = new ClusteredBTSortedPage();
			PageId lookup_currentDirPageId = new PageId();
			ClusteredBTSortedPage lookup_dataPage = new ClusteredBTSortedPage();
			PageId lookup_currentDataPageId = new PageId();
			RID lookup_currentDataPageRid = new RID();

			boolean status = _findDataPage(lastBTPageId,
					lookup_currentDirPageId, lookup_dirPage, 
					lookup_currentDataPageId, lookup_dataPage,
					lookup_currentDataPageRid);

			if(status != true) return null; // record not found 

			atuple = lookup_dirPage.getRecord(lookup_currentDataPageRid);
			dpinfo = new DataPageInfo(atuple);

			if ( dpinfo.availspace >= insert_tuple.getTupleByteArray().length ) 
			{
//				System.out.println("Case where key is largest so far we have seen and last datapage can hold the record");
				RID lookup_rid = lookup_dataPage.lastRecord();
				Tuple lasttuplebytes = lookup_dataPage.getRecord(lookup_rid);
				Tuple lasttuple = TupleUtils.getEmptyTuple(attrType, strsizes);
				lasttuple.tupleCopy(lasttuplebytes);
				lasttuple.print(attrType);
				KeyClass key_to_delete = null;
				key_to_delete = TupleUtils.get_key_from_tuple_attrtype(lasttuple, attrType[key_index-1], key_index);
				/*if ( attrType[key_index-1].attrType == AttrType.attrInteger )
    			 key_to_delete = new IntegerKey(lasttuple.getIntFld(key_index));
	    	  else
	    		  key_to_delete = new StringKey(lasttuple.getStrFld(key_index));
				 */
				btf.Delete(key_to_delete, lookup_rid);

				/* insert the record on the last data page */
				pinPage(lookup_currentDataPageId, lookup_dataPage, false);
				data_inserted_rid = lookup_dataPage.insertRecord(insert_tuple.getTupleByteArray(), attrType, strsizes, key_index);
				btf.insert(key, data_inserted_rid);
				dpinfo.recct++;
				dpinfo.availspace = lookup_dataPage.available_space();
				unpinPage(lookup_currentDataPageId, true);

				/* insert the dpinfo to lookup dir page */
				atuple = lookup_dirPage.returnRecord(lookup_currentDataPageRid);
				DataPageInfo dpinfo_ondirpage = new DataPageInfo(atuple);
				dpinfo_ondirpage.availspace = dpinfo.availspace;
				dpinfo_ondirpage.recct = dpinfo.recct;
				dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;
				dpinfo_ondirpage.flushToTuple();
				unpinPage(lookup_currentDirPageId, true /* = DIRTY */);

				unpinPage(lookup_currentDataPageId,false /*undirty*/);
				unpinPage(currentDirPageId,false /*undirty*/);

			}
			else 
			{
				if ( lookup_dirPage.available_space() >= dpinfo.size ) 
				{
					//System.out.println("Case where we need to insert new data page at the end and no new dir page");
					currentDataPage = _newDatapage(dpinfo);



					atuple = dpinfo.convertToTuple();

					byte [] tmpData = atuple.getTupleByteArray();
					lookup_currentDataPageRid = lookup_dirPage.insertRecord(tmpData);

					data_inserted_rid = currentDataPage.insertRecord(insert_tuple.getTupleByteArray());
					btf.insert(key, data_inserted_rid);

					dpinfo.recct++;
					dpinfo.availspace = currentDataPage.available_space();


					unpinPage(dpinfo.pageId, true /* = DIRTY */);

					atuple = lookup_dirPage.returnRecord(lookup_currentDataPageRid);
					DataPageInfo dpinfo_ondirpage = new DataPageInfo(atuple);


					dpinfo_ondirpage.availspace = dpinfo.availspace;
					dpinfo_ondirpage.recct = dpinfo.recct;
					dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;
					dpinfo_ondirpage.flushToTuple();


					unpinPage(lookup_currentDirPageId, true /* = DIRTY */);

					unpinPage(lookup_currentDataPageId,false /*undirty*/);
					unpinPage(currentDirPageId,false /*undirty*/);
				}
				else {
					//TBD
//					System.out.println("Case where we need to insert a new data page and a new dir page");
					unpinPage(lookup_currentDirPageId, false /* = DIRTY */);
					unpinPage(lookup_currentDataPageId,false /*undirty*/);
					unpinPage(currentDirPageId,false /*undirty*/);

					lookup_currentDirPageId = get_next_empty_slot(); // this page is already pinned
					pinPage(lookup_currentDirPageId, lookup_dirPage, false);

					newDataPage = _newDatapage(newdpinfo);

					//this is going to be the first insert inthe page
					data_inserted_rid = newDataPage.insertRecord(insert_tuple.getTupleByteArray());
					btf.insert(key, data_inserted_rid);
					newdpinfo.availspace = newDataPage.available_space();
					newdpinfo.recct = 1;

					lookup_currentDataPageRid = lookup_dirPage.insertRecord(newdpinfo.returnByteArray());
					unpinPage(lookup_currentDirPageId, true);
					unpinPage(lookup_currentDirPageId, false);
					unpinPage(newdpinfo.pageId, true);
				}
			}
			indScan.DestroyBTreeFileScan();
		}
		else {
			//next entry is not null so we have to insert in between
			//this might leed to a data page splitting and inturn a dir page splitting
			/* 3 cases
			 * 1. No splitting of dir page, Splitting data page
			 * 2. No splitting of dir page, No splitting of data page
			 * 3. Splitting dir page, splitting data page
			 */
			indScan.DestroyBTreeFileScan();
			unpinPage(currentDirPageId, false);
			//System.out.println("We are in the next entry != null scnenario");
			data_inserted_rid = handle_splitting_cases(insert_tuple, attrType, strsizes, key, nextentry, btf, key_index);
		}
		//BT.printBTree(btf.getHeaderPage());
		//BT.printAllLeafPages(btf.getHeaderPage());
		btf.close();

		return data_inserted_rid;

	}

	private void add_page_to_deleted_tuples( ClusteredBTSortedPage datapage) throws Exception {
//		System.out.println("\tadding data to deleted tuples");
		RID temp_delete_rid = datapage.firstRecord();
		while ( temp_delete_rid != null ) {
			Tuple deleted_tuple = datapage.getRecord(temp_delete_rid);
			SystemDefs.JavabaseDB.db_deleted_rids.add( new RID( temp_delete_rid.pageNo, temp_delete_rid.slotNo ) );
			SystemDefs.JavabaseDB.db_deleted_tuples.add(deleted_tuple);
			temp_delete_rid = datapage.nextRecord(temp_delete_rid);
		}

	}

	private void add_page_to_inserted_tuples( ClusteredBTSortedPage datapage) throws Exception {
		//		System.out.println("\tadding data to inserted tuples");
		RID temp_delete_rid = datapage.firstRecord();
		while ( temp_delete_rid != null ) {
			Tuple deleted_tuple = datapage.getRecord(temp_delete_rid);
			SystemDefs.JavabaseDB.db_inserted_rids.add( new RID( temp_delete_rid.pageNo, temp_delete_rid.slotNo ) );
			SystemDefs.JavabaseDB.db_inserted_tuples.add(deleted_tuple);
			temp_delete_rid = datapage.nextRecord(temp_delete_rid);
		}

	}

	private RID handle_splitting_cases( Tuple insert_tuple, 
			AttrType[] attrtype, 
			short[] strsizes, 
			KeyClass key, 
			KeyDataEntry nextentry,
			ClusteredBTreeFile btf,
			int key_index)
					throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFBufMgrException, HFDiskMgrException, Exception
	{
		/* check for which case is this */
		KeyClass key_bt = null;
		if ( key instanceof IntegerKey ) {
			key_bt = ((IntegerKey)nextentry.key);
			//System.out.println("current key value of that page "+((IntegerKey)nextentry.key).getKey());
		}
		else {
			key_bt = ((StringKey)nextentry.key);
			//System.out.println("current key value of that page "+((StringKey)nextentry.key).getKey());
		}
		RID rid_bt = ((LeafData)nextentry.data).getData();
		//System.out.println("Key to be inserted onto page rid"+rid_bt.slotNo+" "+rid_bt.pageNo.pid);
		RID inserted_rid = new RID();

		//look for the page with rid_bt
		ClusteredBTSortedPage lookup_dirPage = new ClusteredBTSortedPage();
		PageId lookup_currentDirPageId = new PageId();
		ClusteredBTSortedPage lookup_dataPage = new ClusteredBTSortedPage();
		PageId lookup_currentDataPageId = new PageId();
		RID lookup_currentDataPageRid = new RID();

		ClusteredBTSortedPage newDataPage = new ClusteredBTSortedPage();

		DataPageInfo dpinfo = new DataPageInfo();     
		DataPageInfo newdpinfo = new DataPageInfo();
		Tuple atuple;

		boolean status = _findDataPage(rid_bt.pageNo,
				lookup_currentDirPageId, lookup_dirPage, 
				lookup_currentDataPageId, lookup_dataPage,
				lookup_currentDataPageRid);


		if ( status == false ) {
			return null;
		}
		else 
		{
			atuple = lookup_dirPage.getRecord(lookup_currentDataPageRid);
			dpinfo = new DataPageInfo(atuple);
			//no splitting dir page case
			if ( lookup_dataPage.available_space() >= insert_tuple.getTupleByteArray().length )
			{
//				System.out.println("Case splitting: No new data page, No new dir page");
				//TBD case 2: No splitting dir page, No splitting data page
				//btf.Delete(key_bt, rid_bt);
				add_page_to_deleted_tuples(lookup_dataPage);
				inserted_rid = lookup_dataPage.insertRecord(insert_tuple.getTupleByteArray(), attrtype, strsizes, key_index);
				add_page_to_inserted_tuples(lookup_dataPage);
//				System.out.println("\tinserted data into the page");

				atuple = lookup_dirPage.returnRecord(lookup_currentDataPageRid);
				DataPageInfo dpinfo_ondirpage = new DataPageInfo(atuple);
				dpinfo_ondirpage.recct++;
				dpinfo_ondirpage.availspace = lookup_dataPage.available_space();
				dpinfo_ondirpage.pageId.pid = inserted_rid.pageNo.pid;
				dpinfo_ondirpage.flushToTuple();


				RID tmprid = lookup_dataPage.lastRecord();
				if ( ( tmprid.pageNo.pid == rid_bt.pageNo.pid ) && ( tmprid.slotNo == rid_bt.slotNo ) )
				{
//					System.out.println("Case 2.1");
					//do nothing
				}
				else
				{
//					System.out.println("Case 2.2");
					//					System.out.println("Key to delete "+key_bt.toString());
					//					System.out.println("RID to delete "+ rid_bt.pageNo.pid + " "+rid_bt.slotNo);
					btf.Delete(key_bt, rid_bt);
					Tuple itr_tuple = lookup_dataPage.getRecord(tmprid);
					Tuple itr_hdr_tuple = TupleUtils.getEmptyTuple(attrtype, strsizes);
					itr_hdr_tuple.tupleCopy(itr_tuple);
					KeyClass key_to_enter = null;
					key_to_enter = TupleUtils.get_key_from_tuple_attrtype(itr_hdr_tuple, attrtype[key_index-1], key_index);
					btf.insert(key_to_enter, tmprid);
				}
				unpinPage(lookup_currentDataPageId, true);
				unpinPage(lookup_currentDirPageId, true);
			}
			else
			{
				if ( lookup_dirPage.available_space() >= dpinfo.size )
				{
//					System.out.println("Case splitting: Insert a new data page and no new dir page");
					// case 1: Data page splits but dir page remains same
					//delete the record for splitting apge from the btree first
					btf.Delete(key_bt, rid_bt);

					newDataPage = _newDatapage(newdpinfo);

					//We already have the directory page where we are going to insert the new dp info pinned in loopkup
					//At this point, we have the new data page pinned, and the old data page pinned lookup_datapage
					//start iterating the old data page and decide where the new data will go
					RID itr_rid = new RID();
					itr_rid = lookup_dataPage.firstRecord();
					boolean found = false;
					int i=0; //slotcnt where new data is to be inserted
					Tuple itr_tuple = new Tuple();
					Tuple itr_hdr_tuple = TupleUtils.getEmptyTuple(attrtype, strsizes);
					while ( ( found!=true ) && ( itr_rid!=null ) )
					{
						itr_tuple = lookup_dataPage.getRecord(itr_rid);
						itr_hdr_tuple.tupleCopy(itr_tuple);
						KeyClass itr_key = null;
						itr_key = TupleUtils.get_key_from_tuple_attrtype(itr_hdr_tuple, attrtype[key_index-1], key_index);
						//						if ( key instanceof IntegerKey )
						//							itr_key = new IntegerKey(itr_hdr_tuple.getIntFld(key_index));
						//						else
						//							itr_key = new StringKey(itr_hdr_tuple.getStrFld(key_index));
						if ( BT.keyCompare(key, itr_key) > 0 ) {
							//System.out.println("Key greater");
							itr_rid = lookup_dataPage.nextRecord(itr_rid);
							i++;
							continue;
						}
						//itr_hdr_tuple.print(attrtype);
						break;
					}

					//now i have the count of tuple which are less than curent insert and the rid of the next
					//tuple bigger than the insert tuple
					RID tmprid = new RID();
					RID copy_itr_rid = new RID(itr_rid.pageNo, itr_rid.slotNo);
					add_page_to_deleted_tuples(lookup_dataPage); // prepare all tuples in old page for removal
					while ( true ) {
						itr_tuple = lookup_dataPage.getRecord(copy_itr_rid);
						itr_hdr_tuple.tupleCopy(itr_tuple);
						tmprid = newDataPage.insertRecord(itr_hdr_tuple.getTupleByteArray(), attrtype, strsizes, key_index);

						/* update the insertion list */
						Tuple inserted_tuple = new Tuple(itr_hdr_tuple.getTupleByteArray(), 0, itr_hdr_tuple.size());
						//						SystemDefs.JavabaseDB.db_inserted_tuples.add(inserted_tuple);
						//						SystemDefs.JavabaseDB.db_inserted_rids.add(new RID(tmprid.pageNo, tmprid.slotNo));

						newdpinfo.recct++;
						newdpinfo.availspace = newDataPage.available_space();
						lookup_dataPage.deleteRecord(copy_itr_rid); // using deleted record because we are still deleting and we dont want rid to change

						/* update the deletion list */
						Tuple deleted_tuple = new Tuple(itr_hdr_tuple.getTupleByteArray(), 0, itr_hdr_tuple.size());
						//						SystemDefs.JavabaseDB.db_deleted_tuples.add(deleted_tuple);
						//						SystemDefs.JavabaseDB.db_deleted_rids.add(new RID(copy_itr_rid.pageNo, copy_itr_rid.slotNo));

						dpinfo.recct--;
						dpinfo.availspace = lookup_dataPage.available_space();
						copy_itr_rid = lookup_dataPage.nextRecord(copy_itr_rid);
						if ( copy_itr_rid == null )
							break;
					}
					lookup_dataPage.compact_slot_dir(); //--> since we did a series of delete, compact the directory
					inserted_rid = lookup_dataPage.insertRecord(insert_tuple.getTupleByteArray());
					dpinfo.recct++;
					dpinfo.availspace = lookup_dataPage.available_space();
					add_page_to_inserted_tuples(newDataPage); // add new page to inserted tuples
					add_page_to_inserted_tuples(lookup_dataPage); // add old page to inserted tuples

					btf.insert(key, inserted_rid);
					itr_tuple = newDataPage.getRecord(newDataPage.lastRecord());
					btf.insert(key_bt, newDataPage.lastRecord());

					atuple = lookup_dirPage.returnRecord(lookup_currentDataPageRid);
					DataPageInfo dpinfo_ondirpage = new DataPageInfo(atuple);
					dpinfo_ondirpage.availspace = dpinfo.availspace;
					dpinfo_ondirpage.recct = dpinfo.recct;
					dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;
					dpinfo_ondirpage.flushToTuple();

					atuple = newdpinfo.convertToTuple();
					byte [] tmpData = atuple.getTupleByteArray();
					tmprid = lookup_dirPage.insertRecord(tmpData);

					unpinPage(lookup_currentDirPageId, true);
					unpinPage(newdpinfo.pageId, true);
					unpinPage(lookup_currentDataPageId, true);
				}
				else
				{
//					System.out.println("Case splitting: Insert a new data page and a new dir page");
					unpinPage(lookup_currentDirPageId, false);
					unpinPage(lookup_currentDataPageId, false);

					lookup_currentDirPageId = get_next_empty_slot(); // this page is already pinned
					pinPage(lookup_currentDirPageId, lookup_dirPage, false);

					newDataPage = _newDatapage(newdpinfo);

					//this is going to be the first insert inthe page
					inserted_rid = newDataPage.insertRecord(insert_tuple.getTupleByteArray());
					btf.insert(key, inserted_rid);
					newdpinfo.availspace = newDataPage.available_space();
					newdpinfo.recct = newDataPage.getSlotCnt();

					atuple = newdpinfo.convertToTuple();

					byte [] tmpData = atuple.getTupleByteArray();
					lookup_currentDataPageRid = lookup_dirPage.insertRecord(tmpData);
					unpinPage(lookup_currentDirPageId, true);
					unpinPage(lookup_currentDirPageId, true);
					unpinPage(newdpinfo.pageId, true);
				}
			}
		}
		return inserted_rid;
	}

	/* returns a pinned page ID */
	private void print_empty_slot() throws Exception {
		boolean found = false;

		RID currentDataPageRid = new RID();
		Page pageinbuffer = new Page();
		ClusteredBTSortedPage currentDirPage = new ClusteredBTSortedPage();
		ClusteredBTSortedPage currentDataPage = new ClusteredBTSortedPage();

		ClusteredBTSortedPage nextDirPage = new ClusteredBTSortedPage(); 
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);
		PageId nextDirPageId = new PageId();  // OK
		PageId prevDirPageId = new PageId();  // OK

		DataPageInfo dpinfo = new DataPageInfo();
		pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);

		while ( ( found == false ) && ( currentDirPageId.pid != INVALID_PAGE ) ) {
//			System.out.println("Looking at data page "+currentDirPageId.pid);
//			System.out.println("Available space i the datapage "+currentDirPage.available_space());
			
			RID temp = currentDirPage.firstRecord();
			
			while ( temp != null ) {
				Tuple atuple = currentDirPage.getRecord(temp);
				DataPageInfo dpinfo1 = new DataPageInfo(atuple);
//				System.out.println("dpinfo recct "+dpinfo1.recct+" "+dpinfo1.pageId.pid);
				temp = currentDirPage.nextRecord(temp);
			}
			prevDirPageId = new PageId(currentDirPageId.pid);
			nextDirPageId = currentDirPage.getNextPage();
			unpinPage(currentDirPageId, false);
			currentDirPageId = new PageId(nextDirPageId.pid);
			if ( currentDirPageId.pid != INVALID_PAGE ) {
				pinPage(currentDirPageId, currentDirPage, false);
			}
		}
	}

	/* returns a pinned page ID */
	private PageId get_next_empty_slot() throws HFBufMgrException, IOException, HFException {
		boolean found = false;

		RID currentDataPageRid = new RID();
		Page pageinbuffer = new Page();
		ClusteredBTSortedPage currentDirPage = new ClusteredBTSortedPage();
		ClusteredBTSortedPage currentDataPage = new ClusteredBTSortedPage();

		ClusteredBTSortedPage nextDirPage = new ClusteredBTSortedPage(); 
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);
		PageId nextDirPageId = new PageId();  // OK
		PageId prevDirPageId = new PageId();  // OK

		DataPageInfo dpinfo = new DataPageInfo();
		pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);

		while ( ( found == false ) && ( currentDirPageId.pid != INVALID_PAGE ) ) {
			if ( currentDirPage.available_space() >= dpinfo.size )
			{
				return currentDirPageId;
			}
			else
			{
				prevDirPageId = new PageId(currentDirPageId.pid);
				nextDirPageId = currentDirPage.getNextPage();
				unpinPage(currentDirPageId, false);
				currentDirPageId = new PageId(nextDirPageId.pid);
				if ( currentDirPageId.pid != INVALID_PAGE ) {
					pinPage(currentDirPageId, currentDirPage, false);
				}
			}
		}


		nextDirPageId = newPage(pageinbuffer, 1);
		// need check error!
		if(nextDirPageId == null)
			throw new HFException(null, "can't new pae");

		// initialize new directory page
		nextDirPage.init(nextDirPageId, pageinbuffer);
		pinPage(prevDirPageId, currentDirPage, false);
		PageId temppid = new PageId(INVALID_PAGE);
		nextDirPage.setNextPage(temppid);
		nextDirPage.setPrevPage(prevDirPageId);
		currentDirPage.setNextPage(nextDirPageId);
		unpinPage(prevDirPageId, true);
		return nextDirPageId;
	}

	/* Internal HeapFile function (used in getRecord and updateRecord):
 returns pinned directory page and pinned data page of the specified 
 user record(rid) and true if record is found.
 If the user record cannot be found, return false.
	 */
	private boolean  _findDataPage( PageId pageno,
			PageId dirPageId, ClusteredBTSortedPage dirpage,
			PageId dataPageId, ClusteredBTSortedPage datapage,
			RID rpDataPageRid) 
					throws InvalidSlotNumberException, 
					InvalidTupleSizeException, 
					HFException,
					HFBufMgrException,
					HFDiskMgrException,
					Exception
	{
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);

		ClusteredBTSortedPage currentDirPage = new ClusteredBTSortedPage();
		ClusteredBTSortedPage currentDataPage = new ClusteredBTSortedPage();
		RID currentDataPageRid = new RID();
		PageId nextDirPageId = new PageId();
		// datapageId is stored in dpinfo.pageId 


		pinPage(currentDirPageId, currentDirPage, false/*read disk*/);

		Tuple atuple = new Tuple();

		while (currentDirPageId.pid != INVALID_PAGE)
		{// Start While01
			// ASSERTIONS:
			//  currentDirPage, currentDirPageId valid and pinned and Locked.

			for( currentDataPageRid = currentDirPage.firstRecord();
					currentDataPageRid != null;
					currentDataPageRid = currentDirPage.nextRecord(currentDataPageRid))
			{
				try{
					atuple = currentDirPage.getRecord(currentDataPageRid);
				}
				catch (InvalidSlotNumberException e)// check error! return false(done) 
				{
					System.out.println("Invalid slot exception");
					return false;
				}

				DataPageInfo dpinfo = new DataPageInfo(atuple);
				try{
					pinPage(dpinfo.pageId, currentDataPage, false/*Rddisk*/);


					//check error;need unpin currentDirPage
				}catch (Exception e)
				{
					unpinPage(currentDirPageId, false/*undirty*/);
					dirpage = null;
					datapage = null;
					throw e;
				}



				// ASSERTIONS:
				// - currentDataPage, currentDataPageRid, dpinfo valid
				// - currentDataPage pinned

				if(dpinfo.pageId.pid==pageno.pid)
				{
					// found user's record on the current datapage which itself
					// is indexed on the current dirpage.  Return both of these.

					dirpage.setpage(currentDirPage.getpage());
					dirPageId.pid = currentDirPageId.pid;

					datapage.setpage(currentDataPage.getpage());
					dataPageId.pid = dpinfo.pageId.pid;

					rpDataPageRid.pageNo.pid = currentDataPageRid.pageNo.pid;
					rpDataPageRid.slotNo = currentDataPageRid.slotNo;
					return true;
				}
				else
				{
					// user record not found on this datapage; unpin it
					// and try the next one
					unpinPage(dpinfo.pageId, false /*undirty*/);

				}

			}

			// if we would have found the correct datapage on the current
			// directory page we would have already returned.
			// therefore:
			// read in next directory page:

			nextDirPageId = currentDirPage.getNextPage();
			try{
				unpinPage(currentDirPageId, false /*undirty*/);
			}
			catch(Exception e) {
				throw new HFException (e, "heapfile,_find,unpinpage failed");
			}

			currentDirPageId.pid = nextDirPageId.pid;
			if(currentDirPageId.pid != INVALID_PAGE)
			{
				pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);
				if(currentDirPage == null)
					throw new HFException(null, "pinPage return null page");  
			}


		} // end of While01
		// checked all dir pages and all data pages; user record not found:(

		dirPageId.pid = dataPageId.pid = INVALID_PAGE;
//		System.out.println("returning false with page not found");
//		System.out.println("reord count " + this.getRecCnt());
		return false;   


	} // end of _findDatapage		     

	public void merge(String btsfilename, AttrType[] attrtype, short[] strsizes, int key_index, int table_tuple_size) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFBufMgrException, HFDiskMgrException, Exception {

		//look for the page with rid_entry
		ClusteredBTSortedPage lookup_dirPage = new ClusteredBTSortedPage();
		PageId lookup_currentDirPageId = new PageId();
		ClusteredBTSortedPage lookup_dataPage = new ClusteredBTSortedPage();
		PageId lookup_currentDataPageId = new PageId();
		RID lookup_currentDataPageRid = new RID();

		//look for the page with rid_next_entry
		ClusteredBTSortedPage nelookup_dirPage = new ClusteredBTSortedPage();
		PageId nelookup_currentDirPageId = new PageId();
		ClusteredBTSortedPage nelookup_dataPage = new ClusteredBTSortedPage();
		PageId nelookup_currentDataPageId = new PageId();
		RID nelookup_currentDataPageRid = new RID();

		ClusteredBTSortedPage newDataPage = new ClusteredBTSortedPage();
		DataPageInfo newdpinfo = new DataPageInfo();

		Tuple atuple_entry, atuple_nextentry, atuple;
		DataPageInfo dpinfo_entry = new DataPageInfo();
		DataPageInfo dpinfo_nextentry = new DataPageInfo();

		ClusteredBTreeFile btf  = new ClusteredBTreeFile(btsfilename);
		BTFileScan indScan = ((ClusteredBTreeFile)btf).new_scan(null, null);
		KeyDataEntry entry = indScan.get_next();
		KeyDataEntry nextentry = null;
		while ( entry!= null ) 
		{
			nextentry = indScan.get_next();
			if ( nextentry == null ) {
				indScan.DestroyBTreeFileScan();
				btf.close();
				break;
			}
			RID rid_nextentry = ((LeafData)nextentry.data).getData();
			RID rid_entry = ((LeafData)entry.data).getData();

			boolean status_entry = _findDataPage(rid_entry.pageNo,
					lookup_currentDirPageId, lookup_dirPage, 
					lookup_currentDataPageId, lookup_dataPage,
					lookup_currentDataPageRid);

			boolean status_nextentry = _findDataPage(rid_nextentry.pageNo,
					nelookup_currentDirPageId, nelookup_dirPage, 
					nelookup_currentDataPageId, nelookup_dataPage,
					nelookup_currentDataPageRid);

			if ( (status_entry == false ) || ( status_nextentry == false ) ) {
				indScan.DestroyBTreeFileScan();
				btf.close();
				if ( ( status_entry == false ) && ( status_nextentry != false ) ) {
					unpinPage(nelookup_currentDirPageId, false);
					unpinPage(nelookup_currentDataPageId, false);
				}
				else if ( ( status_entry != false ) && ( status_nextentry == false ) ) 
				{
					unpinPage(lookup_currentDirPageId, false);
					unpinPage(lookup_currentDataPageId, false);
				}
//				System.err.println("ERROR: Page not found ");
//				System.out.println("Could not find rid  "+ rid_nextentry.pageNo.pid+" "+rid_nextentry.slotNo);
//				System.out.println("Could not find rid  "+ rid_entry.pageNo.pid+" "+rid_entry.slotNo);
				
//				merge(btsfilename, attrtype, strsizes, key_index, table_tuple_size);
				break;
			}

			atuple_entry = lookup_dirPage.getRecord(lookup_currentDataPageRid);
			dpinfo_entry = new DataPageInfo(atuple_entry);

			atuple_nextentry = nelookup_dirPage.getRecord(nelookup_currentDataPageRid);
			dpinfo_nextentry = new DataPageInfo(atuple_nextentry);

			KeyClass key_entry = null;
			KeyClass key_nextentry = null;
			key_nextentry = TupleUtils.get_key_from_key(nextentry.key, attrtype[key_index-1]);
			key_entry = TupleUtils.get_key_from_key(entry.key, attrtype[key_index-1]);
			
			if ( ( dpinfo_entry.recct ) > ( (int)( ( MAX_SPACE - HFPage.DPFIXED )/(2*table_tuple_size) ) ) )
			{
				unpinPage(lookup_currentDirPageId, false);
				unpinPage(nelookup_currentDirPageId, false);
				unpinPage(lookup_currentDataPageId, false);
				unpinPage(nelookup_currentDataPageId, false);
				entry = new KeyDataEntry(key_nextentry, nextentry.data);
				continue;
			}
			if ( ( dpinfo_nextentry.recct ) > ( (int)( ( MAX_SPACE - HFPage.DPFIXED )/(2*table_tuple_size) ) ) )
			{
				unpinPage(lookup_currentDirPageId, false);
				unpinPage(nelookup_currentDirPageId, false);
				unpinPage(lookup_currentDataPageId, false);
				unpinPage(nelookup_currentDataPageId, false);
				entry = new KeyDataEntry(key_nextentry, nextentry.data);
				continue;
			}
			if ( ( dpinfo_entry.recct + dpinfo_nextentry.recct ) > ( (int)( MAX_SPACE - HFPage.DPFIXED )/table_tuple_size ) )
			{
				entry = new KeyDataEntry(key_nextentry, nextentry.data);
				continue;
			}
//			System.out.println("Hitting merge");
			newDataPage = _newDatapage(newdpinfo);
			Tuple itr = new Tuple();
			Tuple itr_hdr = TupleUtils.getEmptyTuple(attrtype, strsizes);
			RID tmprid = lookup_dataPage.firstRecord();
			RID tmp = new RID();
			add_page_to_deleted_tuples(lookup_dataPage); // --> delete all tuples from this page
			while ( tmprid != null )
			{
				itr = lookup_dataPage.getRecord(tmprid);
				itr_hdr.tupleCopy(itr);
				tmp = newDataPage.insertRecord(itr_hdr.getTupleByteArray());
				newdpinfo.recct++;
				/* update the inserted rids */
				Tuple temp_tup = new Tuple(itr_hdr.getTupleByteArray(), 0, itr_hdr.size());
				//				SystemDefs.JavabaseDB.db_inserted_tuples.add(temp_tup);
				//				SystemDefs.JavabaseDB.db_inserted_rids.add(new RID( tmp.pageNo, tmp.slotNo ));

				lookup_dataPage.deleteRecord(tmprid);
				/* update the deleted rids */
				//				SystemDefs.JavabaseDB.db_deleted_tuples.add(temp_tup);
				//				SystemDefs.JavabaseDB.db_deleted_rids.add(new RID( tmprid.pageNo, tmprid.slotNo ));

				tmprid = lookup_dataPage.nextRecord(tmprid);
			}
			tmprid = nelookup_dataPage.firstRecord();
			add_page_to_deleted_tuples(nelookup_dataPage); // --> delete all tuples from this page
			while ( tmprid != null )
			{
				itr = nelookup_dataPage.getRecord(tmprid);
				itr_hdr.tupleCopy(itr);
				tmp = newDataPage.insertRecord(itr_hdr.getTupleByteArray());
				newdpinfo.recct++;
				/* update the inserted rids */
				Tuple temp_tup = new Tuple(itr_hdr.getTupleByteArray(), 0, itr_hdr.size());
				//				SystemDefs.JavabaseDB.db_inserted_tuples.add(temp_tup);
				//				SystemDefs.JavabaseDB.db_inserted_rids.add(new RID( tmp.pageNo, tmp.slotNo ));

				nelookup_dataPage.deleteRecord(tmprid);
				/* update the deleted rids */
				//				SystemDefs.JavabaseDB.db_deleted_tuples.add(temp_tup);
				//				SystemDefs.JavabaseDB.db_deleted_rids.add(new RID( tmprid.pageNo, tmprid.slotNo ));

				tmprid = nelookup_dataPage.nextRecord(tmprid);
			}
			add_page_to_inserted_tuples(newDataPage);// --> add the new data page in inserted tuples
			newdpinfo.availspace = newDataPage.available_space();

			tmprid = newDataPage.lastRecord();
			itr = newDataPage.getRecord(tmprid);
			itr_hdr.tupleCopy(itr);

			KeyClass key_new;
			key_new = TupleUtils.get_key_from_tuple_attrtype(itr_hdr, attrtype[key_index-1], key_index);

			indScan.DestroyBTreeFileScan();
			btf.Delete(key_entry, rid_entry);
			btf.Delete(key_nextentry, rid_nextentry);
			btf.insert(key_new, tmprid);
			btf.close();

			unpinPage(newdpinfo.pageId, true);
			lookup_dirPage.deleteRecord(lookup_currentDataPageRid);
			nelookup_dirPage.deleteRecord(nelookup_currentDataPageRid);

//			System.out.println("Merging pages "+ lookup_currentDataPageRid.pageNo.pid+" and "+nelookup_currentDataPageRid.pageNo.pid);
//			System.out.println("New dp info avail "+ newdpinfo.availspace+" page "+newdpinfo.pageId.pid);
			atuple = newdpinfo.convertToTuple();
			byte [] tmpData = atuple.getTupleByteArray();
			lookup_currentDataPageRid = lookup_dirPage.insertRecord(tmpData);
//			print_empty_slot();
			
			//unpinPage(newdpinfo.pageId, true);
			unpinPage(lookup_currentDirPageId, true);
			unpinPage(nelookup_currentDirPageId, true);
			unpinPage(lookup_currentDataPageId, true);
			unpinPage(nelookup_currentDataPageId, true);
			freePage(nelookup_currentDataPageId);
			freePage(lookup_currentDataPageId);

			merge(btsfilename, attrtype, strsizes, key_index, table_tuple_size);
			break;
		}
	}

	public List<RID> deleteRecord(Tuple delete_tuple, String btsfilename, AttrType[] attrtype, short[] strsizes, int key_index, int a) throws Exception {
		List<RID> list = new ArrayList<>();
		RID temp_rid = deleteRecord(delete_tuple, btsfilename, attrtype, strsizes, key_index);
		while ( temp_rid != null ) {
//			System.out.println("---------temp rid "+temp_rid.pageNo.pid+" "+temp_rid.slotNo);
			temp_rid = deleteRecord(delete_tuple, btsfilename, attrtype, strsizes, key_index);

		}
		return list;
	}
	/** Delete record from file with given rid.
	 *
	 * @exception InvalidSlotNumberException invalid slot number
	 * @exception InvalidTupleSizeException invalid tuple size
	 * @exception HFException heapfile exception
	 * @exception HFBufMgrException exception thrown from bufmgr layer
	 * @exception HFDiskMgrException exception thrown from diskmgr layer
	 * @exception Exception other exception
	 *
	 * @return true record deleted  false:record not found
	 */
	public RID deleteRecord(Tuple delete_tuple, String btsfilename, AttrType[] attrtype, short[] strsizes, int key_index)  
			throws InvalidSlotNumberException, 
			InvalidTupleSizeException, 
			HFException, 
			HFBufMgrException,
			HFDiskMgrException,
			Exception

	{
		RID deleted_rid_temp = null;
		boolean things_deleted = false;

		//look for the page with rid_next_entry
		ClusteredBTSortedPage nelookup_dirPage = new ClusteredBTSortedPage();
		PageId nelookup_currentDirPageId = new PageId();
		ClusteredBTSortedPage nelookup_dataPage = new ClusteredBTSortedPage();
		PageId nelookup_currentDataPageId = new PageId();
		RID nelookup_currentDataPageRid = new RID();

		KeyClass key = null;
		key = TupleUtils.get_key_from_tuple_attrtype(delete_tuple, attrtype[key_index-1], key_index);

		ClusteredBTreeFile btf  = new ClusteredBTreeFile(btsfilename);
		
		BTFileScan indScan = ((ClusteredBTreeFile)btf).new_scan(key, null);
		KeyDataEntry nextentry = indScan.get_next();
		while ( true )
		{
			if ( nextentry == null ) 
			{
//				System.out.println("Next entry turned out to be null");
//				System.out.println("Key "+key.toString());
				indScan.DestroyBTreeFileScan();
				btf.close();
				return deleted_rid_temp;
			}
			else 
			{
//				System.out.println("Next entry not null ");
				//			indScan.DestroyBTreeFileScan();
				RID rid_nextentry = ((LeafData)nextentry.data).getData();
//				System.out.println("RID next entry "+ rid_nextentry.pageNo.pid+" "+rid_nextentry.slotNo);
				KeyClass key_nextentry = null;
				key_nextentry = TupleUtils.get_key_from_key(nextentry.key, attrtype[key_index-1]);
				boolean status_nextentry = _findDataPage(rid_nextentry.pageNo,
						nelookup_currentDirPageId, nelookup_dirPage, 
						nelookup_currentDataPageId, nelookup_dataPage,
						nelookup_currentDataPageRid);
				if ( status_nextentry == false ) {
//					System.out.println("Page not found");
					//					indScan.DestroyBTreeFileScan();
					//					btf.close();
					//					return deleted_rid_temp;

				}
				else 
				{
					RID lookup_rid = null;
					lookup_rid = nelookup_dataPage.firstRecord();
					Tuple atuple_entry = new Tuple();
					Tuple atuple_hdr_entry = TupleUtils.getEmptyTuple(attrtype, strsizes);

					/* see if the tuple belongs here */
					atuple_entry = nelookup_dataPage.getRecord(lookup_rid);
					atuple_hdr_entry.tupleCopy(atuple_entry);
					KeyClass key_lowest = null;
					key_lowest = TupleUtils.get_key_from_key_type(key,  atuple_hdr_entry, key_index);
					if ( BT.keyCompare(key_lowest, key) > 0 ) {
//						System.out.println("Lowest page key bigger than our key");
//						System.out.println("Key "+key_lowest.toString());
						indScan.DestroyBTreeFileScan();
						btf.close();
						unpinPage(nelookup_currentDirPageId, false);
						unpinPage(nelookup_currentDataPageId, false);
						return deleted_rid_temp;
					}
//					System.out.println("Key: "+key.toString());
					DataPageInfo dpinfo_entry = new DataPageInfo();
					atuple_entry = nelookup_dirPage.getRecord(nelookup_currentDataPageRid);
					dpinfo_entry = new DataPageInfo(atuple_entry);
					lookup_rid = nelookup_dataPage.firstRecord();

					/* copy all things from datapage as things to be removed */
					RID temp_delete_rid = nelookup_dataPage.firstRecord();
					add_page_to_deleted_tuples(nelookup_dataPage);

					while ( lookup_rid != null ) {
						atuple_entry = nelookup_dataPage.getRecord(lookup_rid);
						atuple_hdr_entry.tupleCopy(atuple_entry);
						if ( TupleUtils.Equal(atuple_hdr_entry, delete_tuple, attrtype, attrtype.length) ) 
						{
							deleted_rid_temp = new RID( lookup_rid.pageNo, lookup_rid.slotNo);
							things_deleted=true;
							nelookup_dataPage.deleteRecord(lookup_rid);
							dpinfo_entry.availspace = nelookup_dataPage.available_space();
							dpinfo_entry.recct--;
						}
						else {
							//							System.out.println("Not same");
						}
						lookup_rid = nelookup_dataPage.nextRecord(lookup_rid);
					}
					nelookup_dataPage.compact_slot_dir();
					add_page_to_inserted_tuples(nelookup_dataPage);

//					System.out.println("Page slot count " + dpinfo_entry.recct);


					if ( dpinfo_entry.recct == 0 ) {
//						System.out.println("Page empty, deleting the entire page----------------------");
						indScan.DestroyBTreeFileScan();
						btf.Delete(key_nextentry, rid_nextentry);
						nelookup_dirPage.deleteRecord(nelookup_currentDataPageRid);


						btf.close();
						unpinPage(nelookup_currentDataPageId, true);
						freePage(nelookup_currentDataPageId);
						unpinPage(nelookup_currentDirPageId, true);
						return deleted_rid_temp;
					}
					else {
						if ( things_deleted ) {
							indScan.DestroyBTreeFileScan();
							btf.Delete(key_nextentry, rid_nextentry);
							lookup_rid = nelookup_dataPage.lastRecord();
							atuple_entry = nelookup_dataPage.getRecord(lookup_rid);
							atuple_hdr_entry.tupleCopy(atuple_entry);
							KeyClass key_new = null;
							key_new = TupleUtils.get_key_from_key_type(key,  atuple_hdr_entry, key_index);
							btf.insert(key_new, lookup_rid);
							Tuple atuple = nelookup_dirPage.returnRecord(nelookup_currentDataPageRid);
							DataPageInfo dpinfo_ondirpage = new DataPageInfo(atuple);
							dpinfo_ondirpage.availspace = dpinfo_entry.availspace;
							dpinfo_ondirpage.recct = dpinfo_entry.recct;
							dpinfo_ondirpage.pageId.pid = dpinfo_entry.pageId.pid;
							dpinfo_ondirpage.flushToTuple();
						}

						unpinPage(nelookup_currentDirPageId, true);
						unpinPage(nelookup_currentDataPageId, true);
					}
					if ( things_deleted ) {

						//btf.Delete(key_nextentry, rid_nextentry);
//						System.out.println("returning from things deleted true------------");
						btf.close();
						return deleted_rid_temp;
					}
					if ( TupleUtils.are_keys_equal(key_nextentry, key) == false ) {
//						System.out.println("returning from keys not equal-----------------");
						indScan.DestroyBTreeFileScan();
						btf.close();
						return deleted_rid_temp;
					}
				}
			}
			nextentry = indScan.get_next();
		}
	}

	public void delete_empty_directory_pages() {

	}

}// End of HeapFile 












