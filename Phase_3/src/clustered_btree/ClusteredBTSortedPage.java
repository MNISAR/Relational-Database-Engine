/*
 * @(#) SortedPage.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *
 *      by Xiaohu Li (xiaohu@cs.wisc.edu)
 */

package clustered_btree;

import btree.*;
import java.io.*;
import java.lang.*;
import global.*;
import diskmgr.*;
import heap.*;
import iterator.TupleUtils;


/**
 * BTsortedPage class 
 * just holds abstract records in sorted order, based 
 * on how they compare using the key interface from BT.java.
 */
public class ClusteredBTSortedPage  extends HFPage{
  
	public ClusteredBTSortedPage()
    {
      super();
    }
	public ClusteredBTSortedPage(Page page)
    {
      super(page);
    }
	
  /**
   * Performs a sorted insertion of a record on an record page. The records are
   *  sorted in increasing key order.
   *  Only the  slot  directory is  rearranged.  The  data records remain in
   *  the same positions on the  page.
   * 
   *@param entry the entry to be inserted. Input parameter.
   *@return its rid where the entry was inserted; null if no space left.
   *@exception  InsertRecException error when insert
   */
   public RID insertRecord( byte[] tuple, AttrType[] attrtype, short[] strsizes, int key_index)
          throws InsertRecException 
   {
     int i;
     RID rid = new RID();
     // ASSERTIONS:
     // - the slot directory is compressed; Inserts will occur at the end
     // - slotCnt gives the number of slots used
     
     // general plan:
     //    1. Insert the record into the page,
     //       which is then not necessarily any more sorted
     //    2. Sort the page by rearranging the slots (insertion sort)
     
     try {
       rid=super.insertRecord(tuple);
       Tuple t = TupleUtils.getEmptyTuple(attrtype, strsizes);
       int tuple_size = tuple.length;
       if ( rid == null )
    	   return null;
	 
//	 System.out.println("Full Slots in the table "+getSlotCnt());
	 // performs a simple insertion sort
	 for (i=getSlotCnt()-1; i > 0; i--) {
		 KeyClass key_i, key_iplus1;
	     byte[] tuple_i, tuple_iplus1;
//	     System.out.println("\tswapping check for i "+i+" page "+ rid.pageNo.pid);
	     tuple_i = Convert.getbyteValue(getSlotOffset(i), getpage(), getSlotLength(i));
	     tuple_iplus1 = Convert.getbyteValue(getSlotOffset(i-1), getpage(), getSlotLength(i-1));

	     Tuple t_i = new Tuple(tuple_i, 0, tuple_i.length);
	     Tuple t_iplus1 = new Tuple(tuple_iplus1, 0, tuple_iplus1.length);
	     
	     Tuple t_i_hdr = TupleUtils.getEmptyTuple(attrtype, strsizes);
	     Tuple t_iplus1_hdr = TupleUtils.getEmptyTuple(attrtype, strsizes);
	     
	     t_i_hdr.tupleCopy(t_i);
	     t_iplus1_hdr.tupleCopy(t_iplus1);
	     
//	     System.out.print(" Tuple i ");
//	     t_i_hdr.print(attrtype);
//	     System.out.print("Tuple i-1 ");
//	     t_iplus1_hdr.print(attrtype);
	     
	     key_i = TupleUtils.get_key_from_tuple_attrtype(t_i_hdr, attrtype[key_index-1], key_index);
	     key_iplus1 = TupleUtils.get_key_from_tuple_attrtype(t_iplus1_hdr, attrtype[key_index-1], key_index);
	     /*if ( attrtype[key_index-1].attrType == AttrType.attrInteger ) {
	    	 key_i = new IntegerKey(t_i_hdr.getIntFld(key_index));
	    	 key_iplus1 = new IntegerKey(t_iplus1_hdr.getIntFld(key_index));
	     }
	     else {
	    	 key_i = new StringKey(t_i_hdr.getStrFld(key_index));
	    	 key_iplus1 = new StringKey(t_iplus1_hdr.getStrFld(key_index));
	     }*/
//	     System.out.println("Comparing keys "+key_i.toString()+" and "+key_iplus1.toString());
	     if (BT.keyCompare(key_i, key_iplus1) < 0)
	       {
		       // switch slots:
//	    	 System.out.println("swapping");
			 int ln, off;
			 ln= getSlotLength(i);
			 off=getSlotOffset(i);
			 setSlot(i,getSlotLength(i-1),getSlotOffset(i-1));  
			 setSlot(i-1, ln, off);
	       } 
	     else {
		 // end insertion sort
		 break;
	       }
		 
	 }
	 
	 // ASSERTIONS:
	 // - record keys increase with increasing slot number 
	 // (starting at slot 0)
	 // - slot directory compacted
	 
	 rid.slotNo = i;
	 //System.out.println("Sorteg page rid page "+rid.pageNo.pid+" slot "+rid.slotNo);
	 return rid;
     }
     catch (Exception e ) { 
       throw new InsertRecException(e, "insert record failed"); 
     }
     
     
   } // end of insertRecord
 

  /**  Deletes a record from a sorted record page. It also calls
   *    HFPage.compact_slot_dir() to compact the slot directory.
   *@param rid it specifies where a record will be deleted
   *@return true if success; false if rid is invalid(no record in the rid).
   *@exception DeleteRecException error when delete
   */
  public  boolean deleteSortedRecord(RID rid)
    throws DeleteRecException
    {
      try {
	
	deleteRecord(rid);
	compact_slot_dir();
	return true;  
	// ASSERTIONS:
	// - slot directory is compacted
      }
      catch (Exception  e) {
	if (e instanceof InvalidSlotNumberException)
	  return false;
	else
	  throw new DeleteRecException(e, "delete record failed");
      }
    } // end of deleteSortedRecord
  
  /** How many records are in the page
   *@param return the number of records.
   *@exception IOException I/O errors
   */
  public int numberOfRecords() 
    throws IOException
    {
      return getSlotCnt();
    }
};




