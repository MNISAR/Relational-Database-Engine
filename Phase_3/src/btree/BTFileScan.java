/*
 * @(#) BTIndexPage.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu)
 *
 */
package btree;
import java.io.*;

import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.*;
import heap.*;

/**
 * BTFileScan implements a search/iterate interface to B+ tree 
 * index files (class BTreeFile).  It derives from abstract base
 * class IndexFileScan.  
 */
public class BTFileScan  extends IndexFileScan
implements  GlobalConst
{

	public BTreeFile bfile; 
	public String treeFilename;     // B+ tree we're scanning 
	public BTLeafPage leafPage;   // leaf page containing current record
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

	private boolean first_record = true;
	public RID next_rid_scan = null;

	/**
	 * Iterate once (during a scan).  
	 *@return null if done; otherwise next KeyDataEntry
	 *@exception ScanIteratorException iterator error
	 */
	public KeyDataEntry get_next() 
			throws ScanIteratorException
	{

		KeyDataEntry entry;
		PageId nextpage;
		try {
			if (leafPage == null)
				return null;

			if ((deletedcurrent && didfirst) || (!deletedcurrent && !didfirst)) {
				didfirst = true;
				deletedcurrent = false;
				entry=leafPage.getCurrent(curRid);
			}
			else {
				entry = leafPage.getNext(curRid);
			}

			while ( entry == null ) {
				nextpage = leafPage.getNextPage();
				SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), true);
				if (nextpage.pid == INVALID_PAGE) {
					leafPage = null;
					return null;
				}

				leafPage=new BTLeafPage(nextpage, keyType);

				entry=leafPage.getFirst(curRid);
			}

			if (endkey != null)  
				if ( BT.keyCompare(entry.key, endkey)  > 0) {
					// went past right end of scan 
					SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false);
					leafPage=null;
					return null;
				}

			return entry;
		}
		catch ( Exception e) {
			e.printStackTrace();
			throw new ScanIteratorException();
		}
	}


	/**
	 * Delete currently-being-scanned(i.e., just scanned)
	 * data entry.
	 *@exception ScanDeleteException  delete error when scan
	 */
	public void delete_current() 
			throws ScanDeleteException {

		KeyDataEntry entry;
		try{  
			if (leafPage == null) {
				System.out.println("No Record to delete!"); 
				throw new ScanDeleteException();
			}

			if( (deletedcurrent == true) || (didfirst==false) ) 
				return;    

			entry=leafPage.getCurrent(curRid);  
			SystemDefs.JavabaseBM.unpinPage( leafPage.getCurPage(), false);
			bfile.Delete(entry.key, ((LeafData)entry.data).getData());
			leafPage=bfile.findRunStart(entry.key, curRid);

			deletedcurrent = true;
			return;
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new ScanDeleteException();
		}  
	}

	/** max size of the key
	 *@return the maxumum size of the key in BTFile
	 */
	public int keysize() {
		return maxKeysize;
	}  



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
			SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), true);
		} 
		leafPage=null;
	}


	@Override
	public Tuple get_next_tuple() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public KeyDataEntry get_next_entry() {
		// TODO Auto-generated method stub
		try {
			KeyDataEntry entry;
			if ( first_record ) {
				while ( true ) {
					PageId nextLeafPage;

					nextLeafPage = leafPage.getNextPage();
					if ( nextLeafPage.pid != INVALID_PAGE ) {
						SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false);
						leafPage = new BTLeafPage(nextLeafPage, keyType);
					}
					else {
						break;
					}
				}
				first_record = false;
				next_rid_scan = leafPage.lastRecord();
			}
			if ( next_rid_scan != null ) {
				entry = leafPage.getRecordEntry(next_rid_scan);
				next_rid_scan = leafPage.prevRecord(next_rid_scan);
				return entry;
			}
			else {
				PageId nextLeafPage;
				nextLeafPage = leafPage.getPrevPage();
				if ( nextLeafPage.pid != INVALID_PAGE ) {
					SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false);
					leafPage = new BTLeafPage(nextLeafPage, keyType);
					next_rid_scan = leafPage.lastRecord();
				}
				else {
					return null;
				}
				entry = leafPage.getRecordEntry(next_rid_scan);
				next_rid_scan = leafPage.prevRecord(next_rid_scan);
				return entry;
			}

		}catch (IOException e) {
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
		} catch (ConstructPageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IteratorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}



}





