package clustered_btree;
import java.io.*;

import btree.*;
import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.*;
import heap.*;

/**
 * ClBTFileScan implements a search/iterate interface to B+ tree 
 * index files (class BTreeFile).  It derives from abstract base
 * class IndexFileScan.  
 */
public class ClBTFileScanASC  extends IndexFileScan
implements  GlobalConst
{

	public BTreeFile bfile; 
	public String treeFilename;     // B+ tree we're scanning 
	public BTLeafPage leafPage;   // leaf page containing current record
	public BTLeafPage itrleafPage;   // leaf page containing current record
	public RID curRid;       // position in current leaf; note: this is 
	// the RID of the key/RID pair within the
	// leaf page.                                    
	public boolean didfirst;        // false only before getNext is called
	public boolean deletedcurrent;  // true after deleteCurrent is called (read
	// by get_next, written by deleteCurrent).

	public KeyClass endkey;    // if NULL, then go all the way right
	// else, stop when current record > this value.
	// (that is, implement an inclusive range 
	// scan -- the only way to do a search for 
	// a single value).
	public int keyType;
	public int maxKeysize;

	public boolean first_record = true;
	public RID next_rid_scan = null;
	public RID next_data_scan = null;
	public PageId prev_page = new PageId(INVALID_PAGE);
	public HFPage datapage;
	/**
	 * destructor.
	 * unpin some pages if they are not unpinned already.
	 * and do some clearing work.
	 *@exception IOException  error from the lower layer
	 *@exception bufmgr.InvalidFrameNumberException  error from the lower layer
	 *@exception bufmgr.ReplacerException  error from the lower layer
	 *@exception bufmgr.PageUnpinnedException  error from the lower layer
	 *@exception bufmgr.HashEntryNotFoundException   error from the lower layer
	 */
	public  void DestroyBTreeFileScan()
			throws  IOException, bufmgr.InvalidFrameNumberException,bufmgr.ReplacerException,
			bufmgr.PageUnpinnedException,bufmgr.HashEntryNotFoundException   
	{ 
		if (leafPage != null) {
			SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false);
		} 
		if (prev_page != null) {
			SystemDefs.JavabaseBM.unpinPage(prev_page, false);
		} 
		prev_page = null;
		leafPage=null;
	}

	private Tuple next_data_record() throws ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, IOException, HashEntryNotFoundException, InvalidSlotNumberException, IteratorException {
		if ( next_data_scan == null ) {
			KeyDataEntry entry = leafPage.getRecordEntry(next_rid_scan);
			RID rid_data_page = ((LeafData)entry.data).getData();
			datapage = new HFPage();
			SystemDefs.JavabaseBM.pinPage(rid_data_page.pageNo, datapage, false);
			next_data_scan = datapage.firstRecord();
			if ( prev_page.pid == INVALID_PAGE ) {
				prev_page = new PageId(rid_data_page.pageNo.pid);
			}
			else {
				SystemDefs.JavabaseBM.unpinPage(prev_page, false);
				prev_page = new PageId(rid_data_page.pageNo.pid);
			}
		}
		Tuple ret_tuple = datapage.getRecord(next_data_scan);
		next_data_scan = datapage.nextRecord(next_data_scan);
		return ret_tuple;
	}

	@Override
	public Tuple get_next_tuple() {
		// TODO Auto-generated method stub
		try {
			if ( first_record ) {
				first_record = false;
				next_rid_scan = leafPage.firstRecord();
				return next_data_record();
			}
			if ( next_data_scan != null ) {
				return next_data_record();
			}
			else {
				if ( next_rid_scan != null ) {
					RID temp_rid = new RID();
					next_rid_scan = leafPage.nextRecord(next_rid_scan);
					if ( next_rid_scan != null ) {
						return next_data_record();
					}
				}
				PageId nextLeafPage = leafPage.getNextPage();
				if ( (nextLeafPage).pid != INVALID_PAGE ) {
					SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false);
					leafPage = new BTLeafPage(nextLeafPage, keyType);
					next_rid_scan = leafPage.firstRecord();
					return next_data_record();
				}
				else {
					return null;
				}
			}
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ConstructPageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ReplacerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PageUnpinnedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HashEntryNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidFrameNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HashOperationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PageNotReadException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BufferPoolExceededException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PagePinnedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BufMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidSlotNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IteratorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}



	@Override
	public void delete_current() throws ScanDeleteException {
		// TODO Auto-generated method stub
		
	}



	@Override
	public int keysize() {
		// TODO Auto-generated method stub
		return 0;
	}



	@Override
	public KeyDataEntry get_next() throws ScanIteratorException {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public KeyDataEntry get_next_entry() {
		// TODO Auto-generated method stub
		return null;
	}

}
