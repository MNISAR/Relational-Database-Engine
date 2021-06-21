/* File DB.java */

package diskmgr;

import btree.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.lang.Comparable;

import bufmgr.*;
import global.*;
import heap.FieldNumberOutOfBoundException;
import heap.FileAlreadyDeletedException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import iterator.TupleUtils;

public class DB implements GlobalConst {
	
	/* central list of deleted tuples into clustered btrees */
	public List<Tuple> db_deleted_tuples = new ArrayList<>();
	public List<RID> db_deleted_rids = new ArrayList<>();
	
	/* central list of inserted tuples into clustered btrees */
	public List<Tuple> db_inserted_tuples = new ArrayList<>();
	public List<RID> db_inserted_rids = new ArrayList<>();
	
	/* list of all the new key, rid pairs to be added everywhere */
	/* list of all the tables in the database */
	private Queue<Table> tables = new LinkedList<>();
	
	/* name of the heapfile in the current contaning the tables relation */
	private String table_relation;
	
	/* name of the heapfile containing the names of the cols in table */
	private String table_col_names;
	
	/* for storing the headers of the tables */
	AttrType[] names_cols_attr = {new AttrType(AttrType.attrString)};
	
	/* for storing str sizes of the table */
	short[] names_strsizes = {(short) STRSIZE};
	
	/* attrtype of the table relation 
	 * TBD update the relation once we have the clustered and unclustered btree and hash indices
	   * table structure -->
	   * ______________________________________________________________________________________
	   *| tablename   | num_attr  | delim_sep_col_names | delim_sep_col_types | table_row_size |
	   *````````````````````````````````````````````````````````````````````````````````````````
	   **/
	private static AttrType[] tables_attr = {new AttrType(AttrType.attrString), //contains the tablename
											 new AttrType(AttrType.attrInteger),//contains the number of attributes in the table  
											 new AttrType(AttrType.attrInteger),// contains the size of tuple in the table
											 new AttrType(AttrType.attrString), //contains a string of comma separated column names
											 new AttrType(AttrType.attrString), // contains a string of comma separated attrtypes
											 new AttrType(AttrType.attrString), // contains an array of 1s and 0s which says whether an btree unclustered index exist on attr
											 new AttrType(AttrType.attrString),// contains an array of 1s and 0s which says whether an hash unclustered index exist on attr
											 new AttrType(AttrType.attrInteger),//contains the clustered btree attribute number; if -1 then no clustered btree present
											 new AttrType(AttrType.attrInteger)};//contains the clustered hash attribute number; if -1 then no clustered hash present
	private static short[] tables_strsize;
	
	private static int table_relation_tuple_size;
	
	private static final int bits_per_page = MAX_SPACE * 8;
  
  
	/** Load the relation table in memory
	   *
	   *
	   * @exception IOException I/O errors
	   * @exception FileIOException file I/O error
	   * @exception InvalidPageNumberException invalid page number
	   * @exception DiskMgrException error caused by other layers
	   * 
	   * TBD update the relation once we have the clustered and unclustered indices
	   * table structure -->
	   * ______________________________________________________________________________________
	   *| tablename   | num_attr  | delim_sep_col_names | delim_sep_col_types | table_row_size |
	   *````````````````````````````````````````````````````````````````````````````````````````
	   */
	public void read_table_relation() {
		try {
			//System.out.println("reading the tables in DB");
			Heapfile hf = new Heapfile(table_relation);
			Heapfile hf1 = new Heapfile(table_col_names);
			Scan relation_scan = hf.openScan();
			Scan tcn_scan = hf1.openScan();
			//System.out.println("Number of tables in the DB "+hf.getRecCnt());
			Tuple t1 = new Tuple();
			t1.setHdr( (short)tables_attr.length, tables_attr, tables_strsize);
			table_relation_tuple_size = t1.size();
			//System.out.println("tuple size "+table_relation_tuple_size);
			if ( hf.getRecCnt() > 0 ) {
				RID rid = new RID();
				Tuple t, temp_t;
				t = new Tuple(table_relation_tuple_size);
				t.setHdr( (short)tables_attr.length, tables_attr, tables_strsize);
				temp_t = new Tuple(table_relation_tuple_size);
				temp_t.setHdr( (short)tables_attr.length, tables_attr, tables_strsize);
				temp_t = relation_scan.getNext(rid);
				while ( temp_t != null ) {
					t.tupleCopy(temp_t);
					
					/* get the name of the table */
					String tablename = t.getStrFld(1);
					//System.out.println(tablename);
					
					/* get the number of attributes in the table */
					int num_attr = t.getIntFld(2);
					//System.out.println("Number of attributes in table "+num_attr);
					
					/* size of the tuple of the table */
					int tsize = t.getIntFld(3);
					//System.out.println("Size of the tuple in the table is "+tsize);
					
					/* get the column names from the table */
//					String col_string = t.getStrFld(4);
//					String[] col_token = col_string.split(",");
					String[] col_token = new String[num_attr];
					
					RID temp_rid = new RID();
					Tuple temp1 = new Tuple(), temp2 = new Tuple();
					temp1.setHdr((short)names_cols_attr.length, names_cols_attr, names_strsizes);
					for ( int i=0; i<num_attr; i++ ) {
						temp2 = tcn_scan.getNext(temp_rid);
						temp1.tupleCopy(temp2);
						col_token[i] = new String(temp1.getStrFld(1));
					}
					//System.out.println(Arrays.toString(col_token));
					
					/* get the column types */
					AttrType[] col_attr = new AttrType[num_attr];
					String col_attr_str = t.getStrFld(5);
					String[] col_token_attr_str = col_attr_str.split(",");
					for ( int i=0; i<num_attr; i++) {
						col_attr[i] = new AttrType(Integer.parseInt(col_token_attr_str[i]));
					}
					//System.out.println("attrtypes "+Arrays.toString(col_token_attr_str));
					
					/* get the btree unclustered attr boolean array */
					boolean[] bunc = new boolean[num_attr];
					String bunc_str = t.getStrFld(6);
					String[] bunc_token_str = bunc_str.split(",");
					for( int i=0; i<num_attr; i++ ) {
						if ( bunc_token_str[i].equals("1") ) {
							bunc[i] = true;
						}
						else {
							bunc[i] = false;
						}
					}
					
					/* get the hash unclustered attr boolean array */
					boolean[] hunc = new boolean[num_attr];
					String hunc_str = t.getStrFld(7);
					String[] hunc_token_str = hunc_str.split(",");
					for( int i=0; i<num_attr; i++ ) {
						if ( hunc_token_str[i].equals("1") ) {
							hunc[i] = true;
						}
						else {
							hunc[i] = false;
						}
					}
					
					/* get the clustered btree attribute number from the table */
					int b_clustered_attr = t.getIntFld(8);
					
					/* get the clustered hash attribute number from the table */
					int h_clustered_attr = t.getIntFld(9);
					
					Table relation = new Table(tablename, num_attr, col_attr, col_token, tsize, bunc, hunc, b_clustered_attr, h_clustered_attr);
					tables.add(relation);
					
					temp_t = relation_scan.getNext(rid);
				}
				relation_scan.closescan();
				tcn_scan.closescan();
			}
		} catch (HFException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFBufMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFDiskMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTupleSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FieldNumberOutOfBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidSlotNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void add_to_mater_table(Tuple temp, Table mater_table) {
    	try {
    		if ( mater_table == null ) {
    			return;
    		}
			Tuple t = TupleUtils.getEmptyTuple(mater_table.getTable_attr_type(), mater_table.getTable_attr_size());
			t.tupleCopy(temp);
			Heapfile outheapfile = new Heapfile(mater_table.getTable_heapfile());
			RID newrid = outheapfile.insertRecord(t.getTupleByteArray());
		} catch (InvalidTypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTupleSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFBufMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFDiskMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidSlotNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SpaceNotAvailableException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }

  /** Open the database with the given name.
   *
   * @param fname DB_name
   *
   * @exception IOException I/O errors
   * @exception FileIOException file I/O error
   * @exception InvalidPageNumberException invalid page number
   * @exception DiskMgrException error caused by other layers
   */
  public void openDB( String fname)
    throws IOException, 
	   InvalidPageNumberException, 
	   FileIOException,
	   DiskMgrException {
    
    name = fname;
    table_relation = fname+"TRkhusmodi.in";
    table_col_names = fname+"TCNkhusmodi.in";
    
    // Create a random access file
    fp = new RandomAccessFile(fname, "rw");
    
    PageId pageId = new PageId();
    Page apage = new Page();
    pageId.pid = 0;
    
    num_pages = 1;	//temporary num_page value for pinpage to work
    
    pinPage(pageId, apage, false /*read disk*/);
    
    
    DBFirstPage firstpg = new DBFirstPage();
    firstpg.openPage(apage);
    num_pages = firstpg.getNumDBPages();
    
    unpinPage(pageId, false /* undirty*/);
    read_table_relation();
  }
  
  /** default constructor.
   */
  public DB() {
	  tables_strsize = new short[tables_attr.length];
	  for ( int i=0; i<tables_strsize.length; i++ ) {
		  tables_strsize[i] = STRSIZE;
	  }
  }
  
  
  /** DB Constructors.
   * Create a database with the specified number of pages where the page
   * size is the default page size.
   *
   * @param fname DB name
   * @param num_pgs number of pages in DB
   *
   * @exception IOException I/O errors
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException file I/O error
   * @exception DiskMgrException error caused by other layers
   */
  public void openDB( String fname, int num_pgs)
    throws IOException, 
	   InvalidPageNumberException,
	   FileIOException,
	   DiskMgrException {
    
    name = new String(fname);
    num_pages = (num_pgs > 2) ? num_pgs : 2;
    table_relation = fname+"TRkhusmodi.in";
    table_col_names = fname+"TCNkhusmodi.in";
    
    
    File DBfile = new File(name);
    //DBfile.delete();
    if ( DBfile.exists() ) {
    	fp = new RandomAccessFile(DBfile, "rw");
    }
    else {
    	// Create a random access file
    	fp = new RandomAccessFile(fname, "rw");
    	// Make the file num_pages pages long, filled with zeroes.
        fp.seek((long)(num_pages*MINIBASE_PAGESIZE-1));
        fp.writeByte(0);
     // Initialize space map and directory pages.
        
        // Initialize the first DB page
        Page apage = new Page();
        PageId pageId = new PageId();
        pageId.pid = 0;
        pinPage(pageId, apage, true /*no diskIO*/);
        
        DBFirstPage firstpg = new DBFirstPage(apage);
        
        firstpg.setNumDBPages(num_pages);
        unpinPage(pageId, true /*dirty*/);
        
        // Calculate how many pages are needed for the space map.  Reserve pages
        // 0 and 1 and as many additional pages for the space map as are needed.
        int num_map_pages = (num_pages + bits_per_page -1)/bits_per_page;
        
        set_bits(pageId, 1+num_map_pages, 1);
    }
    
    //read_table_relation();

  }
  
  /** Close DB file.
   * @exception IOException I/O errors.
   */
  public void closeDB() throws IOException {
    fp.close();
    tables.clear();
  }
  
  public void add_all_table_to_relation() {
	  try {
		  Heapfile hf = new Heapfile(table_relation);
		  hf.deleteFile();
		  Heapfile hf1 = new Heapfile(table_col_names);
		  hf1.deleteFile();
	  } catch (HFException | HFBufMgrException | HFDiskMgrException | IOException | InvalidTupleSizeException e) {
		  // TODO Auto-generated catch block
		  e.printStackTrace();
	  } catch (InvalidSlotNumberException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	  java.util.Iterator<Table> it = tables.iterator();
	  while ( it.hasNext() ) {
		  Table temp_table = it.next();
		  System.out.println("Flushing table "+temp_table.getTablename()+" to memory...");
		  add_to_relation_tables(temp_table);
	  }
  }
  
  public Table get_relation(String tablename) {
	  //System.out.println("tables size in database "+tables.size());
	  java.util.Iterator<Table> it = tables.iterator();
	  while ( it.hasNext() ) {
		  Table temp_table = it.next();
		  //System.out.println("talbename :"+temp_table.getTablename());
		  if ( temp_table.getTablename().equals(tablename) ) {
			  //System.out.println("Checking " + Arrays.toString(temp_table.getBtree_unclustered_attr()));
			  return temp_table;
		  }
	  }
	  return null;
  }
  
  public void add_to_relation_queue( Table relation ) {
	  tables.add(relation);
  }
  
  public void add_to_relation_tables(Table relation) {
	  //tables.add(relation);
	  Tuple t = new Tuple();
	  try {
		/* set the tuple for the tables relation */
		t.setHdr( (short)tables_attr.length, tables_attr, tables_strsize);
		table_relation_tuple_size = t.size();
		//System.out.println("Tuple size "+ table_relation_tuple_size);
		t = new Tuple(table_relation_tuple_size);
		t.setHdr( (short)tables_attr.length, tables_attr, tables_strsize);
		
		/* prepare all the data for the tuples relation */
		t.setStrFld(1, relation.getTablename()); // setting the table name in the tuple
		
		/* store the number of attrs in the table */
		t.setIntFld(2, relation.getTable_num_attr()); // number of attributes in the table
		
		/* store the tuple size as well */
		t.setIntFld(3, relation.getTable_tuple_size());
		
		/* make a combined attr of all the cols */
		String s = "";
//		String[] names_cols = relation.getTable_attr_name();
//		for ( int i=0; i<names_cols.length; i++ ) {
//			if ( i==0) {
//				s = names_cols[i];
//			}
//			else {
//				s = s + "," + names_cols[i];
//			}
//		}
		//System.out.println("columns: "+ s);
		t.setStrFld(4, s);
		
		/* make a string of all the attrs */
		String y = "";
		AttrType[] cols_attr = relation.getTable_attr_type();
		for ( int i=0; i<cols_attr.length; i++ ) {
			if ( i==0 ) {
				y = Integer.toString(cols_attr[i].attrType);
			}
			else {
				y = y + "," + cols_attr[i].attrType;
			}
		}
		//System.out.println("attrs: "+ y);
		t.setStrFld(5, y);
		
		/* make a string of 0s and 1s for btree unclustered attr index */
		String bunc = "";
		boolean[] cols_bunc = relation.getBtree_unclustered_attr();
		for ( int i=0; i<cols_bunc.length; i++ ) {
			int j;
			if ( cols_bunc[i] ) {
				j = 1;
			}
			else {
				j = 0;
			}
			if ( i==0 ) {
				bunc = Integer.toString(j);
			}
			else {
				bunc = bunc + "," + Integer.toString(j);
			}
		}
		//System.out.println("unclustered index: " + bunc);
		t.setStrFld(6, bunc);
		
		/* make a string of 0s and 1s for hash unclustered attr index */
		String hunc = "";
		boolean[] cols_hunc = relation.getHash_unclustered_attr();
		for ( int i=0; i<cols_hunc.length; i++ ) {
			int j;
			if ( cols_hunc[i] ) {
				j = 1;
			}
			else {
				j = 0;
			}
			if ( i==0 ) {
				hunc = Integer.toString(j);
			}
			else {
				hunc = hunc + "," + Integer.toString(j);
			}
		}
		//System.out.println("unclustered index: " + hunc);
		t.setStrFld(7, hunc);
		
		/* store the clustered btree attribute number */
		t.setIntFld(8, relation.getClustered_btree_attr()); // clustered btree attribute number in the table --> 1,2,3....
		
		/* store the clustered hash attribute number */
		t.setIntFld(9, relation.getClustered_hash_attr()); // clustered hash attribute number in the table --> 1,2,3...
		
		/* insert the tuple into the heapfile */
		//System.out.println(table_relation);
		Heapfile hf = new Heapfile(table_relation);
		RID rid = new RID();
		rid = hf.insertRecord(t.getTupleByteArray());
		
		Heapfile hf1 = new Heapfile(table_col_names);
		String[] names_cols1 = relation.getTable_attr_name();
		Tuple t1 = new Tuple();
		t1.setHdr((short)1, names_cols_attr, names_strsizes);
		
		for ( int i=0; i<names_cols1.length; i++ ) {
			t1.setStrFld(1, names_cols1[i]);
			rid = hf1.insertRecord(t1.getTupleByteArray());
		}
		
		
	} catch (InvalidTypeException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InvalidTupleSizeException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (FieldNumberOutOfBoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (HFException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (HFBufMgrException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (HFDiskMgrException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InvalidSlotNumberException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (SpaceNotAvailableException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }
  
  
  /** Destroy the database, removing the file that stores it. 
   * @exception IOException I/O errors.
   */
  public void DBDestroy() 
    throws IOException {
    
    fp.close();
    File DBfile = new File(name);
    DBfile.delete();
  }
  
  /** Read the contents of the specified page into a Page object
   *
   * @param pageno pageId which will be read
   * @param apage page object which holds the contents of page
   *
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   */
  public  void read_page(PageId pageno, Page apage)
    throws InvalidPageNumberException, 
	   FileIOException, 
	   IOException {

    if((pageno.pid < 0)||(pageno.pid >= num_pages))
      throw new InvalidPageNumberException(null, "BAD_PAGE_NUMBER");
    
    // Seek to the correct page
    fp.seek((long)(pageno.pid *MINIBASE_PAGESIZE));
    
    // Read the appropriate number of bytes.
    byte [] buffer = apage.getpage();  //new byte[MINIBASE_PAGESIZE];
    try{
      fp.read(buffer);
      PCounter.readIncrement();
    }
    catch (IOException e) {
      throw new FileIOException(e, "DB file I/O error");
    }
    
  }
  
  /** Write the contents in a page object to the specified page.
   *
   * @param pageno pageId will be wrote to disk
   * @param apage the page object will be wrote to disk
   *
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   */
  public void write_page(PageId pageno, Page apage)
    throws InvalidPageNumberException, 
	   FileIOException, 
	   IOException {

    if((pageno.pid < 0)||(pageno.pid >= num_pages))
      throw new InvalidPageNumberException(null, "INVALID_PAGE_NUMBER");
    
    // Seek to the correct page
    fp.seek((long)(pageno.pid *MINIBASE_PAGESIZE));
    
    // Write the appropriate number of bytes.
    try{
      fp.write(apage.getpage());
      PCounter.writeIncrement();
    }
    catch (IOException e) {
      throw new FileIOException(e, "DB file I/O error");
    }
    
  }
  
  /** Allocate a set of pages where the run size is taken to be 1 by default.
   *  Gives back the page number of the first page of the allocated run.
   *  with default run_size =1
   *
   * @param start_page_num page number to start with 
   *
   * @exception OutOfSpaceException database is full
   * @exception InvalidRunSizeException invalid run size 
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException DB file I/O errors
   * @exception IOException I/O errors
   * @exception DiskMgrException error caused by other layers
   */
  public void allocate_page(PageId start_page_num)
    throws OutOfSpaceException, 
	   InvalidRunSizeException, 
	   InvalidPageNumberException, 
	   FileIOException, 
	   DiskMgrException,
           IOException {
    allocate_page(start_page_num, 1);
  }
  
  /** user specified run_size
   *
   * @param start_page_num the starting page id of the run of pages
   * @param runsize the number of page need allocated
   *
   * @exception OutOfSpaceException No space left
   * @exception InvalidRunSizeException invalid run size 
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   * @exception DiskMgrException error caused by other layers
   */
  public void allocate_page(PageId start_page_num, int runsize)
    throws OutOfSpaceException, 
	   InvalidRunSizeException, 
	   InvalidPageNumberException, 
	   FileIOException, 
	   DiskMgrException,
           IOException {

    if(runsize < 0) throw new InvalidRunSizeException(null, "Negative run_size");
    
    int run_size = runsize;
    int num_map_pages = (num_pages + bits_per_page -1)/bits_per_page;
    int current_run_start = 0; 
    int current_run_length = 0;
    
    
    // This loop goes over each page in the space map.
    PageId pgid = new PageId();
    byte [] pagebuf;
    int byteptr;
    
    for(int i=0; i< num_map_pages; ++i) {// start forloop01
	
      pgid.pid = 1 + i;
      // Pin the space-map page.
      
      Page apage = new Page();
      pinPage(pgid, apage, false /*read disk*/);
      
      pagebuf = apage.getpage();
      byteptr = 0;
      
      // get the num of bits on current page
      int num_bits_this_page = num_pages - i*bits_per_page;
      if(num_bits_this_page > bits_per_page)
	num_bits_this_page = bits_per_page;
      
      // Walk the page looking for a sequence of 0 bits of the appropriate
      // length.  The outer loop steps through the page's bytes, the inner
      // one steps through each byte's bits.
      
      for(; num_bits_this_page>0 
	    && current_run_length < run_size; ++byteptr) {// start forloop02
	  
	
	Integer intmask = new Integer(1);
	Byte mask = new Byte(intmask.byteValue());
	byte tmpmask = mask.byteValue();
	
	while (mask.intValue()!=0 && (num_bits_this_page>0)
	       &&(current_run_length < run_size))
	  
	  {	      
	    if( (pagebuf[byteptr] & tmpmask ) != 0)
	      {
		current_run_start += current_run_length + 1;
		current_run_length = 0;
	      }
	    else ++current_run_length;
	    
	    
	    tmpmask <<=1;
	    mask = new Byte(tmpmask);
	    --num_bits_this_page;
	  }
	
	
      }//end of forloop02
      // Unpin the space-map page.
      
      unpinPage(pgid, false /*undirty*/);
      
    }// end of forloop01
    
    if(current_run_length >= run_size)
      {
	start_page_num.pid = current_run_start;
	set_bits(start_page_num, run_size, 1);
	
	return;
      }
    
    throw new OutOfSpaceException(null, "No space left");
  }
  
  /** Deallocate a set of pages starting at the specified page number and
   * a run size can be specified.
   *
   * @param start_page_num the start pageId to be deallocate
   * @param run_size the number of pages to be deallocated
   * 
   * @exception InvalidRunSizeException invalid run size 
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   * @exception DiskMgrException error caused by other layers
   */
  public void deallocate_page(PageId start_page_num, int run_size)
    throws InvalidRunSizeException, 
	   InvalidPageNumberException, 
	   IOException, 
	   FileIOException,
	   DiskMgrException {

    if(run_size < 0) throw new InvalidRunSizeException(null, "Negative run_size");
    
    set_bits(start_page_num, run_size, 0);
  }
  
  /** Deallocate a set of pages starting at the specified page number
   *  with run size = 1
   *
   * @param start_page_num the start pageId to be deallocate
   *
   * @exception InvalidRunSizeException invalid run size 
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   * @exception DiskMgrException error caused by other layers
   * 
   */
  public void deallocate_page(PageId start_page_num)
    throws InvalidRunSizeException, 
	   InvalidPageNumberException, 
	   IOException, 
	   FileIOException,
	   DiskMgrException {

    set_bits(start_page_num, 1, 0);
  }
  
  /** Adds a file entry to the header page(s).
   *
   * @param fname file entry name
   * @param start_page_num the start page number of the file entry
   *
   * @exception FileNameTooLongException invalid file name (too long)
   * @exception InvalidPageNumberException invalid page number
   * @exception InvalidRunSizeException invalid DB run size
   * @exception DuplicateEntryException entry for DB is not unique
   * @exception OutOfSpaceException database is full
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   * @exception DiskMgrException error caused by other layers
   */
  public void add_file_entry(String fname, PageId start_page_num)
    throws FileNameTooLongException, 
	   InvalidPageNumberException, 
	   InvalidRunSizeException,
	   DuplicateEntryException,
	   OutOfSpaceException,
	   FileIOException, 
	   IOException, 
	   DiskMgrException {
    
    if(fname.length() >= MAX_NAME)
      throw new FileNameTooLongException(null, "DB filename too long");
    if((start_page_num.pid < 0)||(start_page_num.pid >= num_pages))
      throw new InvalidPageNumberException(null, " DB bad page number");
    
    // Does the file already exist?  
    
    if( get_file_entry(fname) != null) 
      throw new DuplicateEntryException(null, "DB fileentry already exists");
    
    Page apage = new Page();
    
    boolean found = false;
    int free_slot = 0;
    PageId hpid = new PageId();
    PageId nexthpid = new PageId(0);
    DBHeaderPage dp;
    do
      {// Start DO01
	//  System.out.println("start do01");
        hpid.pid = nexthpid.pid;
	
	// Pin the header page
	pinPage(hpid, apage, false /*read disk*/);
        
	// This complication is because the first page has a different
        // structure from that of subsequent pages.
	if(hpid.pid==0)
	  {
	    dp = new DBFirstPage();
	    ((DBFirstPage) dp).openPage(apage);
	  }
	else
	  {
	    dp = new DBDirectoryPage();
	    ((DBDirectoryPage) dp).openPage(apage);
	  }
	
	nexthpid = dp.getNextPage();
	int entry = 0;
	
	PageId tmppid = new PageId();
	while(entry < dp.getNumOfEntries())
	  {
	    dp.getFileEntry(tmppid, entry);
	    if(tmppid.pid == INVALID_PAGE)  break;
	    entry ++;
	  }
	
	if(entry < dp.getNumOfEntries())
	  {
	    free_slot = entry;
	    found = true;
	  }
	else if (nexthpid.pid != INVALID_PAGE)
	  {
	    // We only unpin if we're going to continue looping.
	    unpinPage(hpid, false /* undirty*/);
	  }
	
      }while((nexthpid.pid != INVALID_PAGE)&&(!found)); // End of DO01
    
    // Have to add a new header page if possible.
    if(!found)
      {
	try{
	  allocate_page(nexthpid);
	}
	catch(Exception e){         //need rethrow an exception!!!!
          unpinPage(hpid, false /* undirty*/);
	  e.printStackTrace();
	}
	
	// Set the next-page pointer on the previous directory page.
	dp.setNextPage(nexthpid);
	unpinPage(hpid, true /* dirty*/);
	
	// Pin the newly-allocated directory page.
	hpid.pid = nexthpid.pid;
	
	pinPage(hpid, apage, true/*no diskIO*/);
	dp = new DBDirectoryPage(apage);
	
	free_slot = 0;
      }
    
    // At this point, "hpid" has the page id of the header page with the free
    // slot; "pg" points to the pinned page; "dp" has the directory_page
    // pointer; "free_slot" is the entry number in the directory where we're
    // going to put the new file entry.
    
    dp.setFileEntry(start_page_num, fname, free_slot);
    
    unpinPage(hpid, true /* dirty*/);
    
  }
  
  /** Delete the entry corresponding to a file from the header page(s).
   *
   * @param fname file entry name
   *
   * @exception FileEntryNotFoundException file does not exist
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   * @exception InvalidPageNumberException invalid page number
   * @exception DiskMgrException error caused by other layers
   */
  public void delete_file_entry(String fname)
    throws FileEntryNotFoundException, 
	   IOException,
	   FileIOException,
	   InvalidPageNumberException, 
	   DiskMgrException {
    
    Page apage = new Page();
    boolean found = false;
    int slot = 0;
    PageId hpid = new PageId();
    PageId nexthpid = new PageId(0);
    PageId tmppid = new PageId();
    DBHeaderPage dp;
    
    do
      { // startDO01
        hpid.pid = nexthpid.pid;
	
	// Pin the header page.
	pinPage(hpid, apage, false/*read disk*/);

	// This complication is because the first page has a different
        // structure from that of subsequent pages.
	if(hpid.pid==0)
	  {
	    dp = new DBFirstPage();
	    ((DBFirstPage)dp).openPage(apage);
	  }
	else
	  {
	    dp = new DBDirectoryPage();
	    ((DBDirectoryPage) dp).openPage(apage);
	  }
	nexthpid = dp.getNextPage();
	
	int entry = 0;
	
	String tmpname;
	while(entry < dp.getNumOfEntries())
	  {
	    tmpname = dp.getFileEntry(tmppid, entry);
	    
	    if((tmppid.pid != INVALID_PAGE)&&
	       (tmpname.compareTo(fname) == 0)) break; 
	    entry ++;
	  }
	
        if(entry < dp.getNumOfEntries())
	  {
	    slot = entry;
	    found = true;
	  }
	else
	  {
	    unpinPage(hpid, false /*undirty*/);
	  }
	
      } while((nexthpid.pid != INVALID_PAGE) && (!found)); // EndDO01
    
    if(!found)  // Entry not found - nothing deleted
      throw new FileEntryNotFoundException(null, "DB file not found");
    
    // Have to delete record at hpnum:slot
    tmppid.pid = INVALID_PAGE;
    dp.setFileEntry(tmppid, "\0", slot);
    
    unpinPage(hpid, true /*dirty*/);
    
  }
  
  /** Get the entry corresponding to the given file.
   *
   * @param name file entry name
   *
   * @exception IOException I/O errors
   * @exception FileIOException file I/O error
   * @exception InvalidPageNumberException invalid page number
   * @exception DiskMgrException error caused by other layers
   */
  public PageId get_file_entry(String name)
    throws IOException,
	   FileIOException,
	   InvalidPageNumberException, 
	   DiskMgrException {

    Page apage = new Page();
    boolean found = false;
    int slot = 0;
    PageId hpid = new PageId();
    PageId nexthpid = new PageId(0);
    DBHeaderPage dp;
    
    do
      {// Start DO01
	
	// System.out.println("get_file_entry do-loop01: "+name);
        hpid.pid = nexthpid.pid;
	
        // Pin the header page.
        pinPage(hpid, apage, false /*no diskIO*/);
	
	// This complication is because the first page has a different
        // structure from that of subsequent pages.
	if(hpid.pid==0)
	  {
	    dp = new DBFirstPage();
	    ((DBFirstPage) dp).openPage(apage);
	  }
	else
	  {
	    dp = new DBDirectoryPage();
	    ((DBDirectoryPage) dp).openPage(apage);
	  }
	nexthpid = dp.getNextPage();
	
	int entry = 0;
	PageId tmppid = new PageId();
	String tmpname;
	
	while(entry < dp.getNumOfEntries())
	  {
	    tmpname = dp.getFileEntry(tmppid, entry);
	    
	    if((tmppid.pid != INVALID_PAGE)&&
	       (tmpname.compareTo(name) == 0)) break; 
	    entry ++;
	  }
	if(entry < dp.getNumOfEntries())
	  {
	    slot =  entry;
	    found = true;
	  }
	
	unpinPage(hpid, false /*undirty*/);
	
      }while((nexthpid.pid!=INVALID_PAGE)&&(!found));// End of DO01
    
    if(!found)  // Entry not found - don't post error, just fail.
      {    
	//  System.out.println("entry NOT found");
	return null;
      }
    
    PageId startpid = new PageId();
    dp.getFileEntry(startpid, slot);
    return startpid;
  }
  
  /** Functions to return some characteristics of the database.
   */
  public String db_name(){return name;}
  public int db_num_pages(){return num_pages;}
  public int db_page_size(){return MINIBASE_PAGESIZE;}
  
  /** Print out the space map of the database.
   * The space map is a bitmap showing which
   * pages of the db are currently allocated.
   *
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   * @exception InvalidPageNumberException invalid page number
   * @exception DiskMgrException error caused by other layers
   */
  public void dump_space_map()
    throws DiskMgrException,
	   IOException,
	   FileIOException, 
	   InvalidPageNumberException 
	   
    {
      
      System.out.println ("********  IN DUMP");
      int num_map_pages = (num_pages + bits_per_page -1)/bits_per_page;
      int bit_number = 0;
      
      // This loop goes over each page in the space map.
      PageId pgid = new PageId();
      System.out.println ("num_map_pages = " + num_map_pages);
      System.out.println ("num_pages = " + num_pages);
      for(int i=0; i< num_map_pages; i++)
	{//start forloop01
	  
	  pgid.pid = 1 + i;   //space map starts at page1
	  // Pin the space-map page.
	  Page apage = new Page();
	  pinPage(pgid, apage, false/*read disk*/);
	  
	  // How many bits should we examine on this page?
	  int num_bits_this_page = num_pages - i*bits_per_page;
	  System.out.println ("num_bits_this_page = " + num_bits_this_page);
	  System.out.println ("num_pages = " + num_pages);
	  if ( num_bits_this_page > bits_per_page )
	    num_bits_this_page = bits_per_page;
	  
	  // Walk the page looking for a sequence of 0 bits of the appropriate
	  // length.  The outer loop steps through the page's bytes, the inner
	  // one steps through each byte's bits.
	  
	  int pgptr = 0;
	  byte [] pagebuf = apage.getpage();
	  int mask;
	  for ( ; num_bits_this_page > 0; pgptr ++)
	    {// start forloop02
	      
	      for(mask=1;
		  mask < 256 && num_bits_this_page > 0;
		  mask=(mask<<1), --num_bits_this_page, ++bit_number )
		{//start forloop03
		  
		  int bit = pagebuf[pgptr] & mask;
		  if((bit_number%10) == 0)
		    if((bit_number%50) == 0)
		      {
			if(bit_number>0) System.out.println("\n");
			System.out.print("\t" + bit_number +": ");
		      }
		    else System.out.print(' ');
		  
		  if(bit != 0) System.out.print("1");
		  else System.out.print("0");
		  
		}//end of forloop03
	      
	    }//end of forloop02
	  
	  unpinPage(pgid, false /*undirty*/);
	  
	}//end of forloop01
      
      System.out.println();
      
      
    }
  
  private RandomAccessFile fp;
  private int num_pages;
  private String name;
  
  
  /** Set runsize bits starting from start to value specified
   */
  private void set_bits( PageId start_page, int run_size, int bit )
    throws InvalidPageNumberException, 
	   FileIOException, 
	   IOException, 
	   DiskMgrException {

    if((start_page.pid<0) || (start_page.pid+run_size > num_pages))
      throw new InvalidPageNumberException(null, "Bad page number");
    
    // Locate the run within the space map.
    int first_map_page = start_page.pid/bits_per_page + 1;
    int last_map_page = (start_page.pid+run_size-1)/bits_per_page +1;
    int first_bit_no = start_page.pid % bits_per_page;
    
    // The outer loop goes over all space-map pages we need to touch.
    
    for(PageId pgid = new PageId(first_map_page);
	pgid.pid <= last_map_page;
	pgid.pid = pgid.pid+1, first_bit_no = 0)
      {//Start forloop01
	
        // Pin the space-map page.
	Page pg = new Page();
	
	
	pinPage(pgid, pg, false/*no diskIO*/);
	
	
	byte [] pgbuf = pg.getpage();
	
	// Locate the piece of the run that fits on this page.
	int first_byte_no = first_bit_no/8;
	int first_bit_offset = first_bit_no%8;
	int last_bit_no = first_bit_no + run_size -1;
	
	if(last_bit_no >= bits_per_page )
	  last_bit_no = bits_per_page - 1;
	
        int last_byte_no = last_bit_no / 8;
        
	// This loop actually flips the bits on the current page.
	int cur_posi = first_byte_no;
	for(;cur_posi <= last_byte_no; ++cur_posi, first_bit_offset=0)
	  {//start forloop02
	    
	    int max_bits_this_byte = 8 - first_bit_offset;
	    int num_bits_this_byte = (run_size > max_bits_this_byte?
				      max_bits_this_byte : run_size);
	    
            int imask =1;
	    int temp;
	    imask = ((imask << num_bits_this_byte) -1)<<first_bit_offset;
	    Integer intmask = new Integer(imask);
	    Byte mask = new Byte(intmask.byteValue());
	    byte bytemask = mask.byteValue();
	    
	    if(bit==1)
	      {
	        temp = (pgbuf[cur_posi] | bytemask);
	        intmask = new Integer(temp);
		pgbuf[cur_posi] = intmask.byteValue();
	      }
	    else
	      {
		
		temp = pgbuf[cur_posi] & (255^bytemask);
	        intmask = new Integer(temp);
		pgbuf[cur_posi] = intmask.byteValue();
	      }
	    run_size -= num_bits_this_byte;
	    
	  }//end of forloop02
	
	// Unpin the space-map page.
	
	unpinPage(pgid, true /*dirty*/);
	
      }//end of forloop01
    
  }

  /**
   * short cut to access the pinPage function in bufmgr package.
   * @see bufmgr.pinPage
   */
  private void pinPage(PageId pageno, Page page, boolean emptyPage)
    throws DiskMgrException {

    try {
    	//System.out.println("Pinning the page in DB");
      SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
    }
    catch (Exception e) {
      throw new DiskMgrException(e,"DB.java: pinPage() failed");
    }

  } // end of pinPage

  /**
   * short cut to access the unpinPage function in bufmgr package.
   * @see bufmgr.unpinPage
   */
  private void unpinPage(PageId pageno, boolean dirty)
    throws DiskMgrException {

    try {
    	//System.out.println("Unpinning the page in DB");
      SystemDefs.JavabaseBM.unpinPage(pageno, dirty); 
    }
    catch (Exception e) {
      throw new DiskMgrException(e,"DB.java: unpinPage() failed");
    }

  } // end of unpinPage
  
  
}//end of DB class

/**
 * interface of PageUsedBytes
 */
interface PageUsedBytes
{
  int DIR_PAGE_USED_BYTES = 8 + 8;
  int FIRST_PAGE_USED_BYTES = DIR_PAGE_USED_BYTES + 4;
}

/** Super class of the directory page and first page
 */
class DBHeaderPage implements PageUsedBytes, GlobalConst { 

  protected static final int NEXT_PAGE = 0;
  protected static final int NUM_OF_ENTRIES = 4;
  protected static final int START_FILE_ENTRIES = 8;
  protected static final int SIZE_OF_FILE_ENTRY = 4 + MAX_NAME + 2;
  
  protected byte [] data;
  
  /**
   * Default constructor
   */
  public DBHeaderPage ()
    {  }
  
  /**
   * Constrctor of class DBHeaderPage
   * @param page a page of Page object
   * @param pageusedbytes number of bytes used on the page
   * @exception IOException
   */   
  public DBHeaderPage(Page page, int pageusedbytes)
    throws IOException
    {
      data = page.getpage();
      PageId pageno = new PageId();
      pageno.pid = INVALID_PAGE;
      setNextPage(pageno);
      
      PageId temppid = getNextPage();
      
      int num_entries  = (MAX_SPACE - pageusedbytes) /SIZE_OF_FILE_ENTRY; 
      setNumOfEntries(num_entries);
      
      for ( int index=0; index < num_entries; ++index )
        initFileEntry(INVALID_PAGE,  index);
    }
  
  /**
   * set the next page number
   * @param pageno next page ID 
   * @exception IOException I/O errors
   */
  public void setNextPage(PageId pageno)
    throws IOException
    {
      Convert.setIntValue(pageno.pid, NEXT_PAGE, data);
    }
  
  /**
   * return the next page number
   * @return next page ID
   * @exception IOException I/O errors
   */
  public PageId getNextPage()
    throws IOException
    {
      PageId nextPage = new PageId();
      nextPage.pid= Convert.getIntValue(NEXT_PAGE, data);
      return nextPage;
    }
  
  /**
   * set number of entries on this page
   * @param numEntries the number of entries
   * @exception IOException I/O errors
   */
  
  protected void setNumOfEntries(int numEntries) 
    throws IOException	
    { 
      Convert.setIntValue (numEntries, NUM_OF_ENTRIES, data);
    }
  
  /**
   * return the number of file entries on the page
   * @return number of entries
   * @exception IOException I/O errors
   */  
  public int getNumOfEntries()
    throws IOException
    {
      return Convert.getIntValue(NUM_OF_ENTRIES, data);
    }
  
  /**
   * initialize file entries as empty
   * @param empty invalid page number (=-1)
   * @param entryNo file entry number
   * @exception IOException I/O errors
   */
  private void initFileEntry(int empty, int entryNo)
    throws IOException {
    int position = START_FILE_ENTRIES + entryNo * SIZE_OF_FILE_ENTRY;
    Convert.setIntValue (empty, position, data);
  } 
  
  /**
   * set file entry
   * @param pageNo page ID
   * @param fname the file name
   * @param entryNo file entry number
   * @exception IOException I/O errors
   */  
  public  void setFileEntry(PageId pageNo, String fname, int entryNo)
    throws IOException {

    int position = START_FILE_ENTRIES + entryNo * SIZE_OF_FILE_ENTRY;
    Convert.setIntValue (pageNo.pid, position, data);
    Convert.setStrValue (fname, position +4, data);	
  }
  
  /**
   * return file entry info
   * @param pageNo page Id
   * @param entryNo the file entry number
   * @return file name
   * @exception IOException I/O errors
   */  
  public String getFileEntry(PageId pageNo, int entryNo)
    throws IOException {

    int position = START_FILE_ENTRIES + entryNo * SIZE_OF_FILE_ENTRY;
    pageNo.pid = Convert.getIntValue (position, data);
    if (pageNo.pid == INVALID_PAGE) {
        return "arbitraryPageName";
      }
    return (Convert.getStrValue (position+4, data, MAX_NAME + 2));
  }
  
}

/**
 * DBFirstPage class which is a subclass of DBHeaderPage class
 */
class DBFirstPage extends DBHeaderPage {

  protected static final int NUM_DB_PAGE = MINIBASE_PAGESIZE -4;
  
  /**
   * Default construtor 
   */
  public DBFirstPage()  { super();}
  
  /**
   * Constructor of class DBFirstPage class
   * @param page a page of Page object
   * @exception IOException I/O errors
   */
  public DBFirstPage(Page page)
    throws IOException	
    {
      super(page, FIRST_PAGE_USED_BYTES);
    }
  
  /** open an exist DB first page
   * @param page a page of Page object
   */
  public void openPage(Page page)
    {
      data = page.getpage();
    }
  
  
  /**
   * set number of pages in the DB
   * @param num the number of pages in DB
   * @exception IOException I/O errors
   */
  public void setNumDBPages(int num)
    throws IOException	
    {
      Convert.setIntValue (num, NUM_DB_PAGE, data);
    }
  
  /**
   * return the number of pages in the DB
   * @return number of pages in DB
   * @exception IOException I/O errors
   */
  public int getNumDBPages()
    throws IOException {

    return (Convert.getIntValue(NUM_DB_PAGE, data));
  }
  
}

/**
 * DBDirectoryPage class which is a subclass of DBHeaderPage class
 */
class DBDirectoryPage extends DBHeaderPage  { //implements PageUsedBytes

  /**
   * Default constructor
   */
  public DBDirectoryPage ()  { super(); }
  
  /**
   * Constructor of DBDirectoryPage class
   * @param page a page of Page object
   * @exception IOException
   */
  public DBDirectoryPage(Page page)
    throws IOException
    {
      super(page, DIR_PAGE_USED_BYTES);
    }
  
  /** open an exist DB directory page
   * @param page a page of Page object
   */
  public void openPage(Page page)
    {
      data = page.getpage();
    }
  
}
