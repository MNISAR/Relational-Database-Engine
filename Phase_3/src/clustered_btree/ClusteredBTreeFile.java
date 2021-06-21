/*
 * @(#) bt.java   98/03/24
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu).
 *
 */

package clustered_btree;
import btree.*;
import java.io.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;

/** btfile.java
 * This is the main definition of class BTreeFile, which derives from 
 * abstract base class IndexFile.
 * It provides an insert/delete interface.
 */
public class ClusteredBTreeFile extends BTreeFile 
implements GlobalConst {



	/**  BTreeFile class
	 * an index file with given filename should already exist; this opens it.
	 *@param filename the B+ tree file name. Input parameter.
	 *@exception GetFileEntryException  can not ger the file from DB 
	 *@exception PinPageException  failed when pin a page
	 *@exception ConstructPageException   BT page constructor failed
	 */
	public ClusteredBTreeFile(String filename)
			throws GetFileEntryException,  
			PinPageException, 
			ConstructPageException        
	{      
		super(filename);
	}  


	/**
	 *  if index file exists, open it; else create it.
	 *@param filename file name. Input parameter.
	 *@param keytype the type of key. Input parameter.
	 *@param keysize the maximum size of a key. Input parameter.
	 *@param delete_fashion full delete or naive delete. Input parameter.
	 *           It is either DeleteFashion.NAIVE_DELETE or 
	 *           DeleteFashion.FULL_DELETE.
	 *@exception GetFileEntryException  can not get file
	 *@exception ConstructPageException page constructor failed
	 *@exception IOException error from lower layer
	 *@exception AddFileEntryException can not add file into DB
	 */
	public ClusteredBTreeFile(String filename, int keytype,
			int keysize, int delete_fashion)  
					throws GetFileEntryException, 
					ConstructPageException,
					IOException, 
					AddFileEntryException
	{

		super(filename, keytype, keysize, delete_fashion);
	}

	public void  updateHeader(PageId newRoot, PageId maxPageId)
			throws   IOException, 
			PinPageException,
			UnpinPageException
	{

		BTreeHeaderPage header;
		PageId old_data;


		header= new BTreeHeaderPage( pinPage(headerPageId));

		old_data = headerPage.get_rootId();
		header.set_rootId( newRoot);

		/* bottom rightmost page of the btree */
		header.set_maxPageno(maxPageId);

		// clock in dirty bit to bm so our dtor needn't have to worry about it
		unpinPage(headerPageId, true /* = DIRTY */ );


		// ASSERTIONS:
		// - headerPage, headerPageId valid, pinned and marked as dirty

	}

	public void  updateHeaderMaxEntry(PageId maxPageId)
			throws   IOException, 
			PinPageException,
			UnpinPageException
	{

		BTreeHeaderPage header;
		PageId old_data;


		header= new BTreeHeaderPage( pinPage(headerPageId));

		/* bottom rightmost page of the btree */
		header.set_maxPageno(maxPageId);
		// clock in dirty bit to bm so our dtor needn't have to worry about it
		unpinPage(headerPageId, true /* = DIRTY */ );


		// ASSERTIONS:
		// - headerPage, headerPageId valid, pinned and marked as dirty

	}

	/** insert record with the given key and rid
	 *@param key the key of the record. Input parameter.
	 *@param rid the rid of the record. Input parameter.
	 *@exception  KeyTooLongException key size exceeds the max keysize.
	 *@exception KeyNotMatchException key is not integer key nor string key
	 *@exception IOException error from the lower layer
	 *@exception LeafInsertRecException insert error in leaf page
	 *@exception IndexInsertRecException insert error in index page
	 *@exception ConstructPageException error in BT page constructor
	 *@exception UnpinPageException error when unpin a page
	 *@exception PinPageException error when pin a page
	 *@exception NodeNotMatchException  node not match index page nor leaf page
	 *@exception ConvertException error when convert between revord and byte 
	 *             array
	 *@exception DeleteRecException error when delete in index page
	 *@exception IndexSearchException error when search 
	 *@exception IteratorException iterator error
	 *@exception LeafDeleteException error when delete in leaf page
	 *@exception InsertException  error when insert in index page
	 */    
	public void insert(KeyClass key, RID rid) 
			throws KeyTooLongException, 
			KeyNotMatchException, 
			LeafInsertRecException,   
			IndexInsertRecException,
			ConstructPageException, 
			UnpinPageException,
			PinPageException, 
			NodeNotMatchException, 
			ConvertException,
			DeleteRecException,
			IndexSearchException,
			IteratorException, 
			LeafDeleteException, 
			InsertException,
			IOException

	{
		KeyDataEntry  newRootEntry;

		if (BT.getKeyLength(key) > headerPage.get_maxKeySize())
			throw new KeyTooLongException(null,"");

		if ( key instanceof StringKey ) {
			if ( headerPage.get_keyType() != AttrType.attrString ) {
				throw new KeyNotMatchException(null,"");
			}
		}
		else if ( key instanceof IntegerKey ) {
			if ( headerPage.get_keyType() != AttrType.attrInteger ) {
				throw new KeyNotMatchException(null,"");
			}
		}  
		else if ( key instanceof FloatKey ) {
			if ( headerPage.get_keyType() != AttrType.attrReal ) {
				throw new KeyNotMatchException(null,"");
			}
		} 
		else 
			throw new KeyNotMatchException(null,"");


		// TWO CASES:
		// 1. headerPage.root == INVALID_PAGE:
		//    - the tree is empty and we have to create a new first page;
		//    this page will be a leaf page
		// 2. headerPage.root != INVALID_PAGE:
		//    - we call _insert() to insert the pair (key, rid)


		if ( trace != null )
		{
			trace.writeBytes( "INSERT " + rid.pageNo + " "
					+ rid.slotNo + " " + key + lineSep);
			trace.writeBytes( "DO" + lineSep);
			trace.flush();
		}


		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			PageId newRootPageId;
			BTLeafPage newRootPage;
			RID dummyrid;

			newRootPage=new BTLeafPage( headerPage.get_keyType());
			newRootPageId=newRootPage.getCurPage();


			if ( trace != null )
			{
				trace.writeBytes("NEWROOT " + newRootPageId + lineSep);
				trace.flush();
			}



			newRootPage.setNextPage(new PageId(INVALID_PAGE));
			newRootPage.setPrevPage(new PageId(INVALID_PAGE));


			// ASSERTIONS:
			// - newRootPage, newRootPageId valid and pinned

			newRootPage.insertRecord(key, rid); 

			if ( trace!=null )
			{
				trace.writeBytes("PUTIN node " + newRootPageId+lineSep);
				trace.flush();
			}

			unpinPage(newRootPageId, true); /* = DIRTY */
			updateHeader(newRootPageId, rid.pageNo);

			if ( trace!=null )
			{
				trace.writeBytes("DONE" + lineSep);
				trace.flush();
			}


			return;
		}

		// ASSERTIONS:
		// - headerPageId, headerPage valid and pinned
		// - headerPage.root holds the pageId of the root of the B-tree
		// - none of the pages of the tree is pinned yet


		if ( trace != null )
		{
			trace.writeBytes( "SEARCH" + lineSep);
			trace.flush();
		}

		ClusteredBTreeFile btf;
		/*try {
			btf = new ClusteredBTreeFile(dbname);
			BTFileScan indScan = ((ClusteredBTreeFile)btf).new_scan(key, null);
			KeyDataEntry entry = indScan.get_next();
			if ( entry == null ) {
				updateHeaderMaxEntry(rid.pageNo);
			}
			else {
				boolean max_page_needs_update = false;
				PageId maxPageId = null;
				while ( entry != null ) {
					if ( key instanceof IntegerKey ) {
						//System.out.println("Int entry key "+((IntegerKey)entry.key).getKey());
						//System.out.println("Int entry key* "+((IntegerKey)key).getKey());
						if ( ((IntegerKey) key).getKey().intValue() == ((IntegerKey)entry.key).getKey().intValue() ) {
							maxPageId = rid.pageNo;
							max_page_needs_update = true;
							//System.out.println("Max Page updated\n");
						}
						else {
							break;
						}
					}
					else if ( key instanceof StringKey ) {
						//System.out.println("String entry key "+((StringKey)entry.key).getKey());
						//System.out.println("String entry key* "+((StringKey)key).getKey());
						if ( (((StringKey)key).getKey()).equals(((StringKey)entry.key).getKey()) ) {
							maxPageId = rid.pageNo;
							max_page_needs_update = true;
							//System.out.println("Max Page updated\n");
						}
						else {
							break;
						}
					}
					else if ( key instanceof FloatKey ) {
						//System.out.println("String entry key "+((StringKey)entry.key).getKey());
						//System.out.println("String entry key* "+((StringKey)key).getKey());
						if ( (((FloatKey)key).getKey()).equals(((FloatKey)entry.key).getKey()) ) {
							maxPageId = rid.pageNo;
							max_page_needs_update = true;
							//System.out.println("Max Page updated\n");
						}
						else {
							break;
						}
					}
					entry = indScan.get_next();
				}
				if ( max_page_needs_update  ) {
					updateHeaderMaxEntry(maxPageId);
				}
			}
			indScan.DestroyBTreeFileScan();
			btf.close();

		} catch (GetFileEntryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PinPageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ConstructPageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ScanIteratorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PageUnpinnedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidFrameNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HashEntryNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ReplacerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		newRootEntry= _insert(key, rid, headerPage.get_rootId());

		// TWO CASES:
		// - newRootEntry != null: a leaf split propagated up to the root
		//                            and the root split: the new pageNo is in
		//                            newChildEntry.data.pageNo 
		// - newRootEntry == null: no new root was created;
		//                            information on headerpage is still valid

		// ASSERTIONS:
		// - no page pinned

		if (newRootEntry != null)
		{
			BTIndexPage newRootPage;
			PageId      newRootPageId;
			Object      newEntryKey;

			// the information about the pair <key, PageId> is
			// packed in newRootEntry: extract it

			newRootPage = new BTIndexPage(headerPage.get_keyType());
			newRootPageId=newRootPage.getCurPage();

			// ASSERTIONS:
			// - newRootPage, newRootPageId valid and pinned
			// - newEntryKey, newEntryPage contain the data for the new entry
			//     which was given up from the level down in the recursion


			if ( trace != null )
			{
				trace.writeBytes("NEWROOT " + newRootPageId + lineSep);
				trace.flush();
			}


			newRootPage.insertKey( newRootEntry.key, 
					((IndexData)newRootEntry.data).getData() );


			// the old root split and is now the left child of the new root
			newRootPage.setPrevPage(headerPage.get_rootId());

			unpinPage(newRootPageId, true /* = DIRTY */);

			updateHeader(newRootPageId);

		}


		if ( trace !=null )
		{
			trace.writeBytes("DONE"+lineSep);
			trace.flush();
		}


		return;
	}

	/** create a scan with given keys
	 * Cases:
	 *      (1) lo_key = null, hi_key = null
	 *              scan the whole index
	 *      (2) lo_key = null, hi_key!= null
	 *              range scan from min to the hi_key
	 *      (3) lo_key!= null, hi_key = null
	 *              range scan from the lo_key to max
	 *      (4) lo_key!= null, hi_key!= null, lo_key = hi_key
	 *              exact match ( might not unique)
	 *      (5) lo_key!= null, hi_key!= null, lo_key < hi_key
	 *              range scan from lo_key to hi_key
	 *@param lo_key the key where we begin scanning. Input parameter.
	 *@param hi_key the key where we stop scanning. Input parameter.
	 *@exception IOException error from the lower layer
	 *@exception KeyNotMatchException key is not integer key nor string key
	 *@exception IteratorException iterator error
	 *@exception ConstructPageException error in BT page constructor
	 *@exception PinPageException error when pin a page
	 *@exception UnpinPageException error when unpin a page
	 */
	public ClBTFileScan new_scan_cl(KeyClass lo_key, KeyClass hi_key)
			throws IOException,  
			KeyNotMatchException, 
			IteratorException, 
			ConstructPageException, 
			PinPageException, 
			UnpinPageException

	{
		ClBTFileScan scan = new ClBTFileScan();
		if ( headerPage.get_rootId().pid==INVALID_PAGE) {
			scan.leafPage=null;
			return scan;
		}

		scan.treeFilename=dbname;
		scan.endkey=hi_key;
		scan.didfirst=false;
		scan.deletedcurrent=false;
		scan.curRid=new RID();     
		scan.keyType=headerPage.get_keyType();
		scan.maxKeysize=headerPage.get_maxKeySize();
		scan.bfile=this;

		//this sets up scan at the starting position, ready for iteration
		scan.leafPage=findRunStart( lo_key, scan.curRid);
		return scan;
	}
	
	/** create a scan with given keys
	 * Cases:
	 *      (1) lo_key = null, hi_key = null
	 *              scan the whole index
	 *      (2) lo_key = null, hi_key!= null
	 *              range scan from min to the hi_key
	 *      (3) lo_key!= null, hi_key = null
	 *              range scan from the lo_key to max
	 *      (4) lo_key!= null, hi_key!= null, lo_key = hi_key
	 *              exact match ( might not unique)
	 *      (5) lo_key!= null, hi_key!= null, lo_key < hi_key
	 *              range scan from lo_key to hi_key
	 *@param lo_key the key where we begin scanning. Input parameter.
	 *@param hi_key the key where we stop scanning. Input parameter.
	 *@exception IOException error from the lower layer
	 *@exception KeyNotMatchException key is not integer key nor string key
	 *@exception IteratorException iterator error
	 *@exception ConstructPageException error in BT page constructor
	 *@exception PinPageException error when pin a page
	 *@exception UnpinPageException error when unpin a page
	 */
	public ClBTFileScanASC new_scan_cl_ASC(KeyClass lo_key, KeyClass hi_key)
			throws IOException,  
			KeyNotMatchException, 
			IteratorException, 
			ConstructPageException, 
			PinPageException, 
			UnpinPageException

	{
		ClBTFileScanASC scan = new ClBTFileScanASC();
		if ( headerPage.get_rootId().pid==INVALID_PAGE) {
			scan.leafPage=null;
			return scan;
		}

		scan.treeFilename=dbname;
		scan.endkey=hi_key;
		scan.didfirst=false;
		scan.deletedcurrent=false;
		scan.curRid=new RID();     
		scan.keyType=headerPage.get_keyType();
		scan.maxKeysize=headerPage.get_maxKeySize();
		scan.bfile=this;

		//this sets up scan at the starting position, ready for iteration
		scan.leafPage=findRunStart( lo_key, scan.curRid);
		return scan;
	}
}
