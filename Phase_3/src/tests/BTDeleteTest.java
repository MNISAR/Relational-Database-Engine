package tests;

import java.io.File;
import java.io.IOException;

import btree.AddFileEntryException;
import btree.BT;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.ConvertException;
import btree.DeleteFashionException;
import btree.DeleteRecException;
import btree.FreePageException;
import btree.GetFileEntryException;
import btree.IndexFullDeleteException;
import btree.IndexInsertRecException;
import btree.IndexSearchException;
import btree.InsertException;
import btree.InsertRecException;
import btree.IntegerKey;
import btree.IteratorException;
import btree.KeyClass;
import btree.KeyNotMatchException;
import btree.KeyTooLongException;
import btree.LeafDeleteException;
import btree.LeafInsertRecException;
import btree.LeafRedistributeException;
import btree.NodeNotMatchException;
import btree.PinPageException;
import btree.RecordNotFoundException;
import btree.RedistributeException;
import btree.UnpinPageException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;

public class BTDeleteTest implements GlobalConst {

	public static void main(String[] args) {
		
		System.out.println("Start");
		
		BTDeleteTest thiss = new BTDeleteTest();
		try {
			thiss.BTDeletetesting();
			thiss.BTNaiveDeletetesting();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("End");
	}
	
	public BTDeleteTest() {
		long time=System.currentTimeMillis();
		time=10;
		
		String dbpath = "HASHTEST" + time + ".minibase-db";
		new File(dbpath).delete();
		//SystemDefs.MINIBASE_RESTART_FLAG=true;
		SystemDefs sysdef = new SystemDefs(dbpath, 5000, 100, "Clock");

	}
	
	public void BTDeletetesting() throws GetFileEntryException, ConstructPageException, AddFileEntryException, IOException, KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException, FreePageException, RecordNotFoundException, IndexFullDeleteException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
		BTreeFile btf  = new BTreeFile("test_btree_unclustered.in", AttrType.attrInteger, 4, 1/* delete */);
		System.out.println("\n\n\n************************testing full delete ***************************");
	  for ( int i=0; i<30; i++) {
		  KeyClass key;
		  key = new IntegerKey(2);
		  RID rid = new RID();
		  rid.pageNo.pid = i%10;
		  rid.slotNo = i;
		  btf.insert(key, rid);
	  }
	  for ( int i=0; i<30; i++) {
		  KeyClass key;
		  key = new IntegerKey(3);
		  RID rid = new RID();
		  rid.pageNo.pid = i%10;
		  rid.slotNo = i;
		  btf.insert(key, rid);
	  }
	  for ( int i=0; i<30; i++) {
		  KeyClass key;
		  key = new IntegerKey(2);
		  RID rid = new RID();
		  rid.pageNo.pid = i%10;
		  rid.slotNo = i;
		  btf.Delete(key, rid);
	  }
	  for ( int i=0; i<30; i++) {
		  KeyClass key;
		  key = new IntegerKey(3);
		  RID rid = new RID();
		  rid.pageNo.pid = i%10;
		  rid.slotNo = i;
		  btf.Delete(key, rid);
	  }
	  BT.printAllLeafPages(btf.getHeaderPage());
	  BT.printBTree(btf.getHeaderPage());
	  btf.close();
	}
	
	public void BTNaiveDeletetesting() throws GetFileEntryException, ConstructPageException, AddFileEntryException, IOException, KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException, FreePageException, RecordNotFoundException, IndexFullDeleteException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
		BTreeFile btf  = new BTreeFile("test_btree_unclustered_naive.in", AttrType.attrInteger, 4, 0/* delete */);
		System.out.println("\n\n\n************************testing Naive delete ***************************");
	  for ( int i=0; i<100; i++) {
		  KeyClass key;
		  key = new IntegerKey(2);
		  RID rid = new RID();
		  rid.pageNo.pid = i%10;
		  rid.slotNo = i;
		  btf.insert(key, rid);
	  }
	  for ( int i=0; i<100; i++) {
		  KeyClass key;
		  key = new IntegerKey(3);
		  RID rid = new RID();
		  rid.pageNo.pid = i%10;
		  rid.slotNo = i;
		  btf.insert(key, rid);
	  }
	  for ( int i=0; i<100; i++) {
		  KeyClass key;
		  key = new IntegerKey(2);
		  RID rid = new RID();
		  rid.pageNo.pid = i%10;
		  rid.slotNo = i;
		  btf.Delete(key, rid);
	  }
	  for ( int i=0; i<100; i++) {
		  KeyClass key;
		  key = new IntegerKey(3);
		  RID rid = new RID();
		  rid.pageNo.pid = i%10;
		  rid.slotNo = i;
		  btf.Delete(key, rid);
	  }
	  BT.printAllLeafPages(btf.getHeaderPage());
	  BT.printBTree(btf.getHeaderPage());
	  btf.close();
	}

}
