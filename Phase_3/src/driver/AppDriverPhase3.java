package driver;


import java.io.*;
import java.util.*;

import btree.*;
import bufmgr.*;
import chainexception.ChainException;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import diskmgr.PCounter;
import heap.*;
import global.*;
import hashindex.HashIndexWindowedScan;
import index.IndexException;
import iterator.*;
import iterator.Iterator;
import tests.TestDriver;


//watching point: RID rid, some of them may not have to be newed.

class DriverPhase3 extends TestDriver implements GlobalConst
{
    protected String dbpath;
    protected String logpath;

    private static RID   rid;
    private static Heapfile  f = null;
    private boolean status = OK;
    private static String _fileName;
    private static int[] _pref_list;
    private static int _n_pages;
    private static int COLS;
    private static final String hFile = "hFile.in";
    private static AttrType[] attrType;
    private short[] attrSize;
    // create an iterator by open a file scan
    private static FldSpec[] projlist;
    private static RelSpec rel = new RelSpec(RelSpec.outer);

    private static boolean individualBTreeIndexesCreated;
    private static int _t_size;
    static String dataFile = "";
    private int numberOfDimensions = 0;
    //private Table table;
    
    /* current open database */
    private String open_db_name;
    
    /* is the current db open or close */
    private boolean is_current_db_open;
    
    /* list of all the databases created */
    private Queue<String> list_db_name;
    
    /* sysdef object */
    SystemDefs sysdef;
    
    /* query currently under processing */
    private String query;
    
    /* query tokens */
    private String[] tokens;
    
    public DriverPhase3(){
        super("main");
        list_db_name = new LinkedList<>();
        is_current_db_open = false;
        open_db_name = "";
        dbpath = "MINIBASE.minibase-db";
		logpath = "MINIBASE.minibase-log";
    }
    
    /* function to handle open DB and close DB calls */
    private void handleOpenDB(boolean close_db ) {
    	if ( close_db ) {
    		//TBD close the db properly
    		/*if any db is open, flush it and close it */
    		if ( is_current_db_open ) {
    			close_DB();
    		}
    		else {
    			System.out.println("No DB is open currently");
    		}
    		is_current_db_open = false;
    	}
    	else {
    		if ( validate_token_length(2, "open_database") == false ) {
    			return;
    		}
    		if ( is_current_db_open && open_db_name.equals(tokens[1]) ) {
    			System.out.println(tokens[1] + " DB is already open");
    			return;
    		}
    		boolean db_exists = false;
    		java.util.Iterator<String> it = list_db_name.iterator();
    		while ( it.hasNext() ) {
    			String temp_db_name = it.next();
    			if ( temp_db_name == tokens[1] ) {
    				db_exists = true;
    				break;
    			}
    		}
    		/* close the already opened DB if the new db and old db are not same */
    		if ( is_current_db_open ) {
    			System.out.print("A DB is already open, ");
				close_DB();
    		}
    		
    		//TBD what happened if the DB exists and what happens if it doesn't
    		System.out.println("Loading database "+ tokens[1]);
    		open_DB(tokens[1], db_exists);
    	}
    }
    
    /* opens a new db */
    public void open_DB( String db_name, boolean db_exists ) {
    	if ( db_exists ) {
    		/* open the already existing DB */
    		sysdef = new SystemDefs(db_name,0, NUMBFPAGES,DBREPLACER);
    		//SystemDefs.JavabaseDB.openDB(db_name);
    	}
    	else {
    		File f = new File(db_name);
    		if ( f.exists() ) {
    			sysdef = new SystemDefs(db_name,0, NUMBFPAGES,DBREPLACER);
    		}
    		else {
    			sysdef = new SystemDefs(db_name,NUMDBPAGES, NUMBFPAGES,DBREPLACER);
    		}
    		list_db_name.add(db_name);
    	}
    	open_db_name = db_name;
    	is_current_db_open = true;
    }
    
    /* closes the DB and flushes all pages to disk */
    public void close_DB() {
    	try {
    		System.out.println("Closing DB "+open_db_name);
    		SystemDefs.JavabaseDB.add_all_table_to_relation();
			SystemDefs.JavabaseBM.flushAllPages();
			SystemDefs.JavabaseDB.closeDB();
			is_current_db_open = false;
			open_db_name = "";
		} catch (HashOperationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PageUnpinnedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PagePinnedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PageNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BufMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    /* deletes the DB */
    public void destroy_db() {
    	try {
    		System.out.println("Deleting DB "+open_db_name);
    		for ( int i=0; i<list_db_name.size(); i++ ) {
    			String temp_db = list_db_name.remove();
    			if ( temp_db.equals(open_db_name) ) {
    				continue;
    			}
    			else {
    				list_db_name.add(temp_db);
    			}
    		}
    		SystemDefs.JavabaseDB.closeDB();
    		SystemDefs.JavabaseDB.DBDestroy();
    		open_db_name = "";
    		is_current_db_open = false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    /* This function validates a query and prints appropriate command if wrong */
    public boolean validate_token_length( int req_length, String command_name ) {
    	if ( tokens.length != req_length ) {
    		System.out.println(" Command not recognized");
    		printQueryHelper(command_name);
    		return false;
    	}
    	return true;
    }
    
    /* parses the create_index query for the exact structure 
     * part of task 1/2
     * structure: create_index BTREE/HASH ATT_NO TABLENAME
     * */
    public void parse_create_index() {
    	try {
    		/* limiting memory */
    		/*printing the reads and writes and closing pcounter and also free the BM from the limit */
        	PCounter.initialize();
	    	
	    	boolean btree_type_index = query.contains("BTREE");
	    	boolean hash_type_index = query.contains("HASH");
	    	if ( validate_token_length(4, "create_index") == false ) {
				return;
			}
	    	int index_att_no = Integer.parseInt(tokens[2]);
	    	String tablename = tokens[3];
	    	Table table = SystemDefs.JavabaseDB.get_relation(tablename);
	    	if ( table == null ) {
	    		System.out.println("ERROR: Table does not exist ************");
	    		return;
	    	}
			if ( btree_type_index ) {
				System.out.println("Creating an unclustered btree index on table "+tablename+" on attribute "+index_att_no);
				//TBD create UNCLUSTERED BTREE INDEX on attribute
				if ( table.unclustered_index_exist(index_att_no, "btree") ) {
					System.out.println("ERROR: Unclustered btree index already exists on attribute number *********"+index_att_no);
				}
				else {
					table.create_unclustered_index(index_att_no, "btree");
				}
			}
			else if ( hash_type_index ) {
				System.out.println("Creating an unclustered hash index on table "+tablename+" on attribute "+index_att_no);
				//TBD create UNCLUSTERED HASH INDEX on attribute
				if ( table.unclustered_index_exist(index_att_no, "hash") ) {
					System.out.println("ERROR: Unclustered hash index already exists on attribute number *********"+index_att_no);
				}
				else {
					table.create_unclustered_index(index_att_no, "hash");
				}
			}
			/*printing the reads and writes and closing pcounter and also free the BM from the limit */
	    	System.out.println("Number of Page reads: "+PCounter.get_rcounter());
	    	System.out.println("Number of Page Writes: "+PCounter.get_wcounter());
	    	PCounter.initialize();
    	}catch (ArrayIndexOutOfBoundsException e){
	        validate_token_length(0, "create_index");
	    }
    }
    
    /* parses the create_table query for the exact structure 
     * part of task 1/2
     * structure: create_table [CLUSTERED BTREE/HASH ATT_NO] FILENAME
     * */
    public void parse_create_table() throws Exception {
    	
    	/*printing the reads and writes and closing pcounter and also free the BM from the limit */
    	PCounter.initialize();
    	
    	boolean is_index_required = query.contains("CLUSTERED");
    	boolean btree_type_index = query.contains("BTREE");
    	boolean hash_type_index = query.contains("HASH");
    	int index_att_no = -1;
    	String filename;
    	
    	if ( is_index_required ) {
    		if ( validate_token_length(5, "create_table") == false ) {
    			return;
    		}
    		index_att_no = Integer.parseInt(tokens[3]);
    		filename = tokens[4];
    		if ( btree_type_index ) {
    			System.out.println("Creating a table of file "+ filename+" and a clustered btree index on attribute "+index_att_no);
    			//TBD create CLUSTERED BTREE INDEX on attribute
    			Table table = new Table(filename);
        		if ( SystemDefs.JavabaseDB.get_relation(table.getTablename()) != null ) {
        			System.out.println("Error: Table already exists**************");
        			return;
        		}
        		table.create_clustered_table(index_att_no, "btree");
    		}
    		else if ( hash_type_index ) {
    			System.out.println("Creating a table of file "+ filename+" and a clustered hash index on attribute "+index_att_no);
    			//TBD create CLUSTERED HASH INDEX on attribute
    			Table table = new Table(filename);
        		if ( SystemDefs.JavabaseDB.get_relation(table.getTablename()) != null ) {
        			System.out.println("Error: Table already exists**************");
        			return;
        		}
        		table.create_clustered_table(index_att_no, "hash");
    		}
    	}
    	else {
    		if ( validate_token_length(2, "create_table") == false ) {
    			return;
    		}
    		filename = tokens[1];
    		System.out.println("Creating a table of file "+ filename);
    		Table table = new Table(filename);
    		if ( SystemDefs.JavabaseDB.get_relation(table.getTablename()) != null ) {
    			System.out.println("Error: Table already exists**************");
    			return;
    		}
    		//TBD create just a table and no index 
    		table.create_table(table.getTable_heapfile(), table.getTable_data_file());
    		
    	}
    	/*printing the reads and writes and closing pcounter and also free the BM from the limit */
    	System.out.println("Number of Page reads: "+PCounter.get_rcounter());
    	System.out.println("Number of Page Writes: "+PCounter.get_wcounter());
    	PCounter.initialize();
    }
    
    /* parses the insert_data query for the exact structure 
     * part of task 
     * structure: insert_data TABLENAME FILENAME
     * */
    public void parse_insert_data() throws Exception {
    	if ( validate_token_length(3, "insert_data") == false ) {
			return;
		}
    	/*printing the reads and writes and closing pcounter and also free the BM from the limit */
    	System.out.println("Number of Page reads: "+PCounter.get_rcounter());
    	System.out.println("Number of Page Writes: "+PCounter.get_wcounter());
    	PCounter.initialize();
    	String filename = tokens[2];
    	String tablename = tokens[1];
    	System.out.println("Inserting data from file "+filename+" to table "+tablename);
    	//TBD insert the data into mentioned table and update the index structures accordingly
    	Table table = SystemDefs.JavabaseDB.get_relation(tablename);
    	if ( table == null ) {
    		System.out.println("Table does not exist. Please use the create_table query to insert data");
    		printQueryHelper("create_table");
    		return;
    	}
    	table.insert_data(filename);
    	/*printing the reads and writes and closing pcounter and also free the BM from the limit */
    	System.out.println("Number of Page reads: "+PCounter.get_rcounter());
    	System.out.println("Number of Page Writes: "+PCounter.get_wcounter());
    	PCounter.initialize();
    }
    
    /* parses the delete_data query for the exact structure 
     * part of task 
     * structure: delete_data TABLENAME FILENAME 
     * */
    public void parse_delete_data() {
    	if ( validate_token_length(3, "delete_data") == false ) {
			return;
		}
    	/*printing the reads and writes and closing pcounter and also free the BM from the limit */
    	System.out.println("Number of Page reads: "+PCounter.get_rcounter());
    	System.out.println("Number of Page Writes: "+PCounter.get_wcounter());
    	PCounter.initialize();
    	
    	String filename = tokens[2];
    	String tablename = tokens[1];
    	System.out.println("Deleting data of file "+filename+" from table "+tablename);
    	//TBD delete the data from the mentioned table and update the index structures accordingly
    	Table table = SystemDefs.JavabaseDB.get_relation(tablename);
    	if ( table == null ) {
    		System.err.println("ERROR: Table does not exist**");
    		return;
    	}
    	table.delete_data(filename);
    	/*printing the reads and writes and closing pcounter and also free the BM from the limit */
    	System.out.println("Number of Page reads: "+PCounter.get_rcounter());
    	System.out.println("Number of Page Writes: "+PCounter.get_wcounter());
    	PCounter.initialize();
    }
    
    /* parses the output_table query for the exact structure 
     * part of task 
     * structure: output_table TABLENAME
     * */
    public void parse_output_table() {
    	if ( validate_token_length(2, "output_table") == false ) {
			return;
		}
    	/*printing the reads and writes and closing pcounter and also free the BM from the limit */
    	PCounter.initialize();
    	
    	String tablename = tokens[1];
    	System.out.println("Printing the table "+tablename);
    	try {
			Table table = SystemDefs.JavabaseDB.get_relation(tablename);
			if ( table == null ) {
				System.out.println("*********ERROR: table does not exist **************");
			}
			else {
				table.print_table_cl();
			}
		} catch (InvalidTupleSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	/*printing the reads and writes and closing pcounter and also free the BM from the limit */
    	System.out.println("Number of Page reads: "+PCounter.get_rcounter());
    	System.out.println("Number of Page Writes: "+PCounter.get_wcounter());
    	PCounter.initialize();
    }
    
    /* parses the output_index query for the exact structure 
     * part of task 
     * structure: output_index TABLENAME ATT_NO
     * */
    public void parse_output_index() {
    	if ( validate_token_length(3, "output_index") == false ) {
			return;
		}
    	/*printing the reads and writes and closing pcounter and also free the BM from the limit */
    	PCounter.initialize();
    	String tablename = tokens[1];
    	int index_att_no = Integer.parseInt(tokens[2]);
    	System.out.println("Printing the indices on attribute "+index_att_no+" on table "+tablename);
    	Table table = SystemDefs.JavabaseDB.get_relation(tablename);
    	if ( table == null ) {
    		System.err.println("ERROR: Table does not exist**");
    		return;
    	}
    	try {
			table.print_index(index_att_no);
		} catch (GetFileEntryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PinPageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ConstructPageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (IteratorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HashEntryNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidFrameNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PageUnpinnedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ReplacerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	/*printing the reads and writes and closing pcounter and also free the BM from the limit */
    	System.out.println("Number of Page reads: "+PCounter.get_rcounter());
    	System.out.println("Number of Page Writes: "+PCounter.get_wcounter());
    	PCounter.initialize();
    }
    
    /* parses the skyline query for the exact structure 
     * part of phase 2
     * structure: skyline NLS/BNLS/SFS/BTS/BTSS {ATT_NO1, ...ATT_NOh} TABLENAME NPAGES [MATER OUTTABLENAME]
     * */
    public void parse_skyline() {
    	try {
	    	/* ----------------------which skyline needs to be calculated ------------------------------------*/
	    	String skyline_algo = tokens[1];//NLS/BNLS/SFS/BTS/BTSS
	    	
	    	/*---------------------extract tablename and outtablename--------------------------*/
	    	boolean is_output_saved = query.contains("MATER");
	    	
	    	int index_mater = -1;
	    	int skyline_n_pages = 0;
	    	String skyline_tablename;
	    	String out_tablename = "";
	    	String temp_sub_query = "";
	    	
	    	if ( is_output_saved ) {
	    		index_mater = Arrays.asList(tokens).indexOf("MATER");
	    		out_tablename = tokens[index_mater+1];
	    		skyline_tablename = tokens[index_mater-2];
	    		skyline_n_pages = Integer.parseInt( tokens[index_mater-1] );
	    		
	    		tokens = Arrays.copyOfRange(tokens, 2, index_mater-2);
	    		temp_sub_query = String.join(" ", tokens);
	    	}
	    	else {
	    		index_mater = tokens.length - 1;
	    		skyline_tablename = tokens[index_mater-1];
	    		skyline_n_pages = Integer.parseInt( tokens[index_mater] );
	    		
	    		tokens = Arrays.copyOfRange(tokens, 2, index_mater-1);
	    		temp_sub_query = String.join(" ", tokens);
	    	}
	    	
	    	/* --------------------------extract the preference list and n pages from the query------------ */
	    	String temp_query = String.valueOf(temp_sub_query);
	    	temp_query = temp_query.replaceAll("[^\\d]", " ");
	    	temp_query = temp_query.trim();
	    	temp_query = temp_query.replaceAll(" +", " ");
	    	//System.out.println(temp_query);
	    	String[] temp_tokens = temp_query.split(" ");
	    	
	    	/* -----------------------extract the preference list form the token array------------- */
	    	int[] skyline_preference_list = new int[temp_tokens.length];
	    	for ( int pref_count = 0; pref_count < skyline_preference_list.length; pref_count++ ) {
	    		skyline_preference_list[pref_count] = Integer.parseInt(temp_tokens[pref_count]);
	    	}
    	
	    	if ( is_output_saved ) {
	    		System.out.println("Calculating "+skyline_algo+" skyline on table "+ skyline_tablename+" on attributes "+Arrays.toString(skyline_preference_list)+
	    				" with buffer pages: "+skyline_n_pages+". Saving the output table to "+out_tablename);
	    	}
	    	else {
	    		System.out.println("Calculating "+skyline_algo+" skyline on table "+ skyline_tablename+" on attributes "+Arrays.toString(skyline_preference_list)+
	    				" with buffer pages: "+skyline_n_pages);
	    	}
	    	
	    	/* initialising the pcounter and limiting system pages */
	    	SystemDefs.JavabaseBM.limit_memory_usage(true, skyline_n_pages);
	    	PCounter.initialize();
	    	
	    	
	    	/* run the appropriate skyline algorithm */
	    	SkylineQueryDriver skyd = new SkylineQueryDriver(skyline_preference_list, skyline_tablename, skyline_n_pages, out_tablename, skyline_algo);
	    	
	    	/*printing the reads and writes and closing pcounter and also free the BM from the limit */
	    	System.out.println("Number of Page reads: "+PCounter.get_rcounter());
	    	System.out.println("Number of Page Writes: "+PCounter.get_wcounter());
	    	SystemDefs.JavabaseBM.limit_memory_usage(false, skyline_n_pages);
	    	PCounter.initialize();
	    	
    	}catch (ArrayIndexOutOfBoundsException e){
	        validate_token_length(0, "skyline");
	    }
    }
    
    /* parses the groupby query for the exact structure 
     * part of task
     * structure: groupby SORT/HASH MAX/MIN/AGG/SKY G_ATT_NO{ATT_NO1,...ATT_NOh} TABLENAME NPAGES [MATER OUTTABLENAME]
     * */
    public void parse_groupby() throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
    	try {
	    	/* ----------------------which GROUPBY needs to be calculated ------------------------------------*/
	    	String group_algo = tokens[1]; //SORT/HASH
	    	
	    	/* ---------------------which aggregation needs to be used----------------------------------*/
	    	String agg_algo = tokens[2];
	    	
	    	/*---------------------extract tablename and outtablename--------------------------*/
	    	boolean is_output_saved = query.contains("MATER");
	    	int index_mater = -1;
	    	int groupby_n_pages = 0;
	    	String groupby_tablename;
	    	String out_tablename = "";
	    	String temp_sub_query = "";
	    	
	    	if ( is_output_saved ) {
	    		index_mater = Arrays.asList(tokens).indexOf("MATER");
	    		out_tablename = tokens[index_mater+1];
	    		groupby_tablename = tokens[index_mater-2];
	    		groupby_n_pages = Integer.parseInt(tokens[index_mater-1]);
	    		tokens = Arrays.copyOfRange(tokens, 2, index_mater-2);
	    		temp_sub_query = String.join(" ", tokens); 
	    	}
	    	else {
	    		index_mater = tokens.length - 1;
	    		groupby_tablename = tokens[tokens.length-2];
	    		groupby_n_pages = Integer.parseInt(tokens[index_mater]);
	    		tokens = Arrays.copyOfRange(tokens, 2, index_mater-1);
	    		temp_sub_query = String.join(" ", tokens);
	    	}
	    	
	    	/* limiting memory */
	    	SystemDefs.JavabaseBM.limit_memory_usage(true, groupby_n_pages);
	    	PCounter.initialize();
	    	
	    	/* --------------------------extract the preference list and n pages from the query------------ */
	    	String temp_query = String.valueOf(temp_sub_query);
	    	temp_query = temp_query.replaceAll("[^\\d]", " ");
	    	temp_query = temp_query.trim();
	    	temp_query = temp_query.replaceAll(" +", " ");
	    	String[] temp_tokens = temp_query.split(" ");
	    	
	    	/*------------------------extract the groupby attributes ----------------------------*/
	    	int groupby_attribute = Integer.parseInt(temp_tokens[0]);
	    	
	    	/* --------------------------extract the preference list and n pages from the query------------ */
	    	int[] agg_attributes = new int[temp_tokens.length-1];
	    	
	    	/* ------------------extract the aggregation attributes list form the token array------------- */
	    	for ( int pref_count = 0; pref_count < agg_attributes.length; pref_count++ ) {
	    		agg_attributes[pref_count] = Integer.parseInt(temp_tokens[pref_count+1]);
	    	}
	    	
	    	if ( is_output_saved ) {
	    		System.out.println("Calculating groupby "+group_algo+" on table "+ groupby_tablename+" on attribute "+ groupby_attribute +
	    				" with buffer pages: "+groupby_n_pages+ " and aggregation type "+ agg_algo + " on attributes" + Arrays.toString(agg_attributes) +
	    				". Saving the output table to "+out_tablename);
	    	}
	    	else {
	    		System.out.println("Calculating groupby "+group_algo+" on table "+ groupby_tablename+" on attribute "+ groupby_attribute +
	    				" with buffer pages: "+groupby_n_pages+ " and aggregation type "+ agg_algo + " on attributes" + Arrays.toString(agg_attributes));
	    	}
	    	AggType agg = null;
	    	if ( agg_algo.equals("MIN") ) {
	    		agg = new AggType(AggType.MIN);
	    	}
	    	else if ( agg_algo.equals("MAX") ) {
	    		agg = new AggType(AggType.MAX);
	    	}
	    	else if ( agg_algo.equals("SKY") ) {
	    		agg = new AggType(AggType.SKYLINE);
	    	}
	    	else if ( agg_algo.equals("AVG") ) {
	    		agg = new AggType(AggType.AVG);
	    	}
	    	else {
	    		agg = null;
	    	}
	    	
	    	Table groupby_table = SystemDefs.JavabaseDB.get_relation(groupby_tablename);
	    	if ( ( agg == null ) || ( groupby_table == null ) ) {
	    		validate_token_length(0, "groupby");
	    		System.err.println("***ERROR in the Query***");
	    		return;
	    	}
	    	Iterator groupby = null;
	    	FldSpec[] groupby_projection = get_projection_for_table(groupby_table, "outer", -1);
			FileScan groupby_table_file_scan =  new FileScan(groupby_table.getTable_heapfile(),
														   	 groupby_table.getTable_attr_type(),
														   	 groupby_table.getTable_attr_size(),
														   	 (short) groupby_table.getTable_num_attr(),
														   	 (short) groupby_table.getTable_num_attr(),
														   	 groupby_projection, 
														   	 null);
			FldSpec[] agg_fldspc = get_aggregation_list(agg_attributes);
			FldSpec groupByAttr = new FldSpec(new RelSpec(RelSpec.outer), groupby_attribute);



			/* keep the output attrtype ready */
			AttrType[] agg_attrtype = new AttrType[groupby_table.getTable_num_attr()];
			for ( int i=0; i<agg_attrtype.length; i++ ) {
				agg_attrtype[i] = new AttrType( (groupby_table.getTable_attr_type())[i].attrType );
			}
			
			for (int i=0; i<agg_attributes.length; i++ ) {
				agg_attrtype[agg_attributes[i] - 1] = new AttrType(AttrType.attrReal);
			}
			
			HashIndexWindowedScan hiwfs = null;
	    	/* run the appropriate groupby algorithm */
	    	switch ( group_algo ) {
	    		case "HASH":
	    			//TBD run HASH groupby with proper params
	    			if ( groupby_table.unclustered_index_exist(groupby_attribute, "hash") == false ) {
	    				groupby_table.create_unclustered_index(groupby_attribute, "hash");
	    			}
	    			hiwfs = new HashIndexWindowedScan(new IndexType(IndexType.Hash),
	    											  groupby_table.getTable_heapfile(), 
	    											  groupby_table.get_unclustered_index_filename(groupby_attribute, "hash"), 
	    											  groupby_table.getTable_attr_type(),
	    											  groupby_table.getTable_attr_size(), 
	    											  groupby_table.getTable_num_attr(), 
	    											  groupby_projection.length,
	    											  groupby_projection, 
	    											  null, 
	    											  groupby_attribute,//confirm that this is groupby attr
	    											  false);

					groupby_projection = new FldSpec[agg_fldspc.length + 1];
					groupby_projection[0] = groupByAttr;

					for(int i=1; i<groupby_projection.length; i++){
						groupby_projection[i] = agg_fldspc[i-1];
					}

					groupby = new GroupByWithHash(groupby_table.getTable_attr_type(),
												  groupby_table.getTable_num_attr(),
												  groupby_table.getTable_attr_size(),
												  hiwfs,
												  groupByAttr,
												  agg_fldspc,
												  agg,
												  groupby_projection,
												  groupby_projection.length,
												  groupby_n_pages);
	    			break;
	    		case "SORT":
					groupby_projection = new FldSpec[agg_fldspc.length + 1];
					groupby_projection[0] = new FldSpec(new RelSpec(RelSpec.outer), groupByAttr.offset);

					for(int i=1; i<groupby_projection.length; i++){
						groupby_projection[i] = new FldSpec(new RelSpec(RelSpec.outer), agg_fldspc[i-1].offset);
					}

	    			groupby = new GroupByWithSort(groupby_table.getTable_attr_type(),
	    										  groupby_table.getTable_num_attr(),
	    										  groupby_table.getTable_attr_size(),
	    										  groupby_table_file_scan,
	    										  groupByAttr,
	    										  agg_fldspc,
	    										  agg,
							                      groupby_projection,
							                      groupby_projection.length,
	    										  groupby_n_pages);
	    			break;
	    		default:
	    			validate_token_length(0, "groupby");
	    			break;
	    	}
	    	List<Tuple> result = new ArrayList<>();
			try {
                result = groupby.get_next_aggr();
            }
            catch (Exception e) {
                status = false;
                e.printStackTrace();
            }

            while(result != null) {
				Iterator finalGroupby = groupby;
				result.forEach((tuple) -> {
                    try {
                        tuple.print(finalGroupby._outAttrType);
                        
                    } catch (IOException e) {
                    	
                        e.printStackTrace();
                    }
                });

                try {
                    result = groupby.get_next_aggr();
                }
                catch (Exception e) {
                    status = false;
                    e.printStackTrace();
                }
            }
            groupby.close();
            /*printing the reads and writes and closing pcounter and also free the BM from the limit */
	    	System.out.println("Number of Page reads: "+PCounter.get_rcounter());
	    	System.out.println("Number of Page Writes: "+PCounter.get_wcounter());
	    	SystemDefs.JavabaseBM.limit_memory_usage(false, groupby_n_pages);
	    	PCounter.initialize();
    	}catch (Exception e){
	        e.printStackTrace();
	    }
    }
    
    private FldSpec[] get_aggregation_list( int[] pref_list) {
    	FldSpec[] fldspc = new FldSpec[pref_list.length];
    	RelSpec rel = new RelSpec(RelSpec.outer);
    	for ( int i=0; i<pref_list.length; i++ ) {
    		fldspc[i] = new FldSpec(rel, pref_list[i]);
    	}
    	return fldspc;
    }
    
    /* parses the join query for the exact structure 
     * part of task
     * structure: join NLJ/SMJ/INLJ/HJ OTABLENAME O_ATT_NO ITABLENAME I_ATT_NO OP NPAGES [MATER OUTTABLENAME]
     * */
    public void parse_join() throws Exception {
    	try {
	    	/* ----------------------which join needs to be calculated ------------------------------------*/
	    	String join_algo = tokens[1]; //NLJ/SMJ/INLJ/HJ
	    	
	    	/* ---------------------which is the outer table----------------------------------*/
	    	String outer_table_name = tokens[2];
	    	Table outer_table = SystemDefs.JavabaseDB.get_relation(outer_table_name);
	    	
	    	/* ---------------------which is the inner table----------------------------------*/
	    	String inner_table_name = tokens[4];
	    	Table inner_table = SystemDefs.JavabaseDB.get_relation(inner_table_name);
	    	
	    	/*--------------------------no table should be null -------------------------------*/
	    	if ( ( outer_table == null ) || ( inner_table == null ) ) {
	    		System.out.println("Inner table or outer table does not exist.");
	    		return;
	    	}
	    	
	    	/*----------------------get the outer nad inner attribute on which to perform the join */
	    	int outer_table_attribute = Integer.parseInt(tokens[3]);
	    	int inner_table_attribute = Integer.parseInt(tokens[5]);
	    	
	    	/*-------------------- operator used for join ----------------------*/
	    	String op = tokens[6];
	    	
	    	/*------------join n_pages----------------------------*/
	    	int join_n_pages = Integer.parseInt(tokens[7]);
	    	SystemDefs.JavabaseBM.limit_memory_usage(true, join_n_pages);
	    	PCounter.initialize();
	    	
	    	/*---------------------extract tablename and outtablename--------------------------*/
	    	boolean is_output_saved = query.contains("MATER");
	    	String out_tablename = "";
	    	Table mater_table = null;
	    	if ( is_output_saved ) {
	    		out_tablename = tokens[9];
	    		mater_table = new Table(out_tablename, "MATER");
	    	}
	    	
	    	if ( is_output_saved ) {
	    		System.out.println(" calculating "+join_algo+" on outer table "+ outer_table_name+" attribute "+ outer_table_attribute +
	    				" and inner table "+ inner_table_name+" attribute "+ inner_table_attribute +
	    				" with buffer pages: "+join_n_pages+ " and operator type "+op+
	    				". Saving the output table to "+out_tablename);
	    	}
	    	else {
	    		System.out.println(" calculating "+join_algo+" on outer table "+ outer_table_name+" attribute "+ outer_table_attribute +
	    				" and inner table "+ inner_table_name+" attribute "+ inner_table_attribute +
	    				" with buffer pages: "+join_n_pages+ " and operator type "+op);
	    	}
	    	CondExpr[] outFilter = get_op_cond_expr(op, outer_table_attribute, inner_table_attribute);
	    	if ( outFilter == null ) {
	    		validate_token_length(0, "join");
	    		return;
	    	}
	    	AttrType[] join_attr_type = get_join_attr_type(outer_table, inner_table, inner_table_attribute);
	    	short[] outer_str_sizes = outer_table.getTable_attr_size();
	    	short[] inner_str_sizes = inner_table.getTable_attr_size();
	    	FldSpec[] outer_projection = get_projection_for_table(outer_table, "outer", -1);
	    	FldSpec[] inner_complete_projection = get_projection_for_table(inner_table, "inner", -1);
			FldSpec[] inner_projection = get_projection_for_table(inner_table, "inner", inner_table_attribute);
			inner_table.inner_projection = inner_complete_projection;
			outer_table.inner_projection = outer_projection;
			FldSpec[] join_projection = get_projection_for_join_table(outer_projection, inner_projection);
			FileScan outer_table_file_scan =  new FileScan(outer_table.getTable_heapfile(), 
										   				   outer_table.getTable_attr_type(),
										   				   outer_table.getTable_attr_size(),
										   				   (short) outer_table.getTable_num_attr(),
										   				   (short) outer_table.getTable_num_attr(),
										   				   outer_projection, 
										   				   null);
			FileScan inner_table_file_scan =  new FileScan(inner_table.getTable_heapfile(), 
														   inner_table.getTable_attr_type(),
														   inner_table.getTable_attr_size(),
										   				   (short) inner_table.getTable_num_attr(),
										   				   (short) inner_table.getTable_num_attr(),
										   				   inner_complete_projection, 
										   				   null);
			
			String[] join_col_names = get_join_attr_name(outer_table, inner_table, inner_table_attribute);
			/* fill in the output_table */
			if ( mater_table != null ) {
				mater_table.setTable_attr_type(join_attr_type);
				mater_table.setTable_num_attr(join_attr_type.length);
				mater_table.intialise_table_str_sizes();
				mater_table.intialise_table_bunc();
				mater_table.intialise_table_hunc();
				mater_table.setTable_attr_name(join_col_names);
			}
			Iterator joiner = null;
	    	/* run the appropriate skyline algorithm */
	    	switch ( join_algo ) {
	    		case "NLJ":
	    			System.out.println("Printing results of Nested Loops Join ---->");
	    			joiner = new NestedLoopsJoins (outer_table.getTable_attr_type(),
	    										   outer_table.getTable_num_attr(), 
	    										   outer_table.getTable_attr_size(),
	    										   inner_table.getTable_attr_type(),
	    										   inner_table.getTable_num_attr(),
	    										   inner_table.getTable_attr_size(),
	    										   join_n_pages,
	    										   outer_table_file_scan, 
	    										   inner_table.getTable_heapfile(),
	    										   outFilter,
	    										   null,
	    										   join_projection,
	    										   join_projection.length);
	    			//TBD run NLJ with proper params
	    			break;
	    		case "SMJ":
	    			System.out.println("Printing results of Sort Merge Join ---->");
	    			TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
	    			joiner = new SortMerge(outer_table.getTable_attr_type(),
										   outer_table.getTable_num_attr(), 
										   outer_table.getTable_attr_size(),
										   inner_table.getTable_attr_type(),
										   inner_table.getTable_num_attr(),
										   inner_table.getTable_attr_size(),
										   outer_table_attribute,
										   outer_str_sizes[outer_table_attribute-1],
										   inner_table_attribute,
										   inner_str_sizes[inner_table_attribute-1],
										   10,
										   outer_table_file_scan, 
										   inner_table_file_scan,
										   false, 
										   false, 
										   ascending,
										   outFilter, 
										   join_projection,
										   join_projection.length);
	    			//TBD run SMJ with proper params
	    			break;
	    		case "INLJ":
	    			System.out.println("Printing results of Index Nested Loops Join ---->");
	    			joiner = new IndexNestedLoopJoin(outer_table.getTable_attr_type(),
	    											 outer_table.getTable_num_attr(),
	    											 outer_table.getTable_attr_size(),
	    											 inner_table.getTable_attr_type(),
	    											 inner_table.getTable_num_attr(),
	    											 inner_table.getTable_attr_size(),
	    											 join_n_pages,
	    											 outer_table_file_scan,
	    											 inner_table.getTable_heapfile(),
	    											 outFilter,
	    											 null,
	    											 join_projection,
	    											 join_projection.length);
	    			//TBD run INLJ with proper params
	    			break;
	    		case "HJ":
	    			System.out.println("Printing results of Hash Join ---->");
	    			joiner = new HashJoin(outer_table.getTable_attr_type(),
									      outer_table.getTable_num_attr(),
									      outer_table.getTable_attr_size(),
									      inner_table.getTable_attr_type(),
									      inner_table.getTable_num_attr(),
									      inner_table.getTable_attr_size(),
									      join_n_pages,
									      outer_table_file_scan,
									      inner_table.getTable_heapfile(),
									      outFilter,
									      null,
									      join_projection,
									      join_projection.length);
	    			//TBD run HJ with proper params
	    			break;
	    		default:
	    			validate_token_length(0, "join");
	    			break;
	    	}
	    	int num_out = 0;
	    	Tuple temp = joiner.get_next();
//	    	temp.printTuple(join_attr_type);
	    	if ( mater_table != null ) {
	    		mater_table.setTable_tuple_size(temp.size());
	    	}
			while ( temp != null ) {
				num_out++;
//				if ( num_out%100 == 0 ) {
//					System.out.println("iter: "+num_out);
//				}
				if ( mater_table == null )
					temp.print(join_attr_type);
				SystemDefs.JavabaseDB.add_to_mater_table(temp, mater_table);
				temp = joiner.get_next();
			}
			
			if ( outer_table_file_scan.closeFlag == false ) {
				outer_table_file_scan.close();
			}
			if ( inner_table_file_scan.closeFlag == false ) {
				inner_table_file_scan.close();
			}
			
			if ( mater_table != null ) {
				System.out.println("");
				mater_table.add_table_to_global_queue();
				mater_table.print_table_cl();
			}
			System.out.println("Number of join elements "+num_out);
			/*printing the reads and writes and closing pcounter and also free the BM from the limit */
	    	System.out.println("Number of Page reads: "+PCounter.get_rcounter());
	    	System.out.println("Number of Page Writes: "+PCounter.get_wcounter());
	    	
	    	PCounter.initialize();
	    	try {
	    		joiner.close();
	    		SystemDefs.JavabaseBM.limit_memory_usage(false, join_n_pages);
	    	} catch ( Exception e ) {
	    		
	    	}
    	}catch (ArrayIndexOutOfBoundsException e){
	        validate_token_length(0, "join");
	    }
    }
    
    private String[] get_join_attr_name( Table outer_table, Table inner_table, int inner_join_attr ) {
    	String[] join_attr_name = new String[outer_table.getTable_num_attr() + inner_table.getTable_num_attr() -1];
    	String[] outer_attr_name = outer_table.getTable_attr_name();
    	String[] inner_attr_name = inner_table.getTable_attr_name();
    	for ( int i=0; i<outer_table.getTable_num_attr(); i++ ) {
    		join_attr_name[i] = outer_attr_name[i];
    	}
    	for ( int i=0; i<(inner_join_attr - 1); i++ ) {
    		join_attr_name[outer_attr_name.length+i] = inner_attr_name[i];
    	}
    	for ( int i=inner_join_attr; i<inner_attr_name.length; i++ ) {
    		join_attr_name[outer_attr_name.length+i-1] = inner_attr_name[i];
    	}
    	
    	return join_attr_name;
    }
    
    private AttrType[] get_join_attr_type( Table outer_table, Table inner_table, int inner_join_attr ) {
    	AttrType[] join_attr_type = new AttrType[outer_table.getTable_num_attr() + inner_table.getTable_num_attr() -1];
    	AttrType[] outer_attr_type = outer_table.getTable_attr_type();
    	AttrType[] inner_attr_type = inner_table.getTable_attr_type();
    	for ( int i=0; i<outer_table.getTable_num_attr(); i++ ) {
    		join_attr_type[i] = outer_attr_type[i];
    	}
    	for ( int i=0; i<(inner_join_attr - 1); i++ ) {
    		join_attr_type[outer_attr_type.length+i] = inner_attr_type[i];
    	}
    	for ( int i=inner_join_attr; i<inner_attr_type.length; i++ ) {
    		join_attr_type[outer_attr_type.length+i-1] = inner_attr_type[i];
    	}
    	
    	return join_attr_type;
    }
    
    private CondExpr[] get_op_cond_expr( String op, int outer_table_join_attr, int inner_table_join_attr ) {
    	CondExpr[] expr = new CondExpr[2];
    	expr[0] = new CondExpr();
    	expr[1] = new CondExpr();
    	switch (op) {
    	case "=":
    		expr[0].next = null;
            expr[0].op = new AttrOperator(AttrOperator.aopEQ);

            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), outer_table_join_attr);

            expr[0].type2 = new AttrType(AttrType.attrSymbol);
            expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), inner_table_join_attr);

            expr[1] = null;
    		break;
    	case "<":
    		expr[0].next = null;
            expr[0].op = new AttrOperator(AttrOperator.aopLT);

            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), outer_table_join_attr);

            expr[0].type2 = new AttrType(AttrType.attrSymbol);
            expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), inner_table_join_attr);

            expr[1] = null;
    		break;
    	case ">":
    		expr[0].next = null;
            expr[0].op = new AttrOperator(AttrOperator.aopGT);

            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), outer_table_join_attr);

            expr[0].type2 = new AttrType(AttrType.attrSymbol);
            expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), inner_table_join_attr);

            expr[1] = null;
    		break;
    	case "<=":
    		expr[0].next = null;
            expr[0].op = new AttrOperator(AttrOperator.aopLE);

            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), outer_table_join_attr);

            expr[0].type2 = new AttrType(AttrType.attrSymbol);
            expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), inner_table_join_attr);

            expr[1] = null;
    		break;
    	case ">=":
    		expr[0].next = null;
            expr[0].op = new AttrOperator(AttrOperator.aopGE);

            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), outer_table_join_attr);

            expr[0].type2 = new AttrType(AttrType.attrSymbol);
            expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), inner_table_join_attr);

            expr[1] = null;
    		break;
    	default:
    		System.out.println("Define the operator correctly");
    		return null;
    	}
    	return expr;
    }
    
    private FldSpec[] get_projection_for_table( Table proj_table, String table_type, int join_attr ) {
    	FldSpec[] proj;
    	if ( table_type.equals("outer") ) {
    		proj = new FldSpec[proj_table.getTable_num_attr()];
        	RelSpec rel = new RelSpec(RelSpec.outer);
        	for ( int i=0; i<proj_table.getTable_num_attr(); i++ ) {
        		proj[i] = new FldSpec(rel, i+1);
        	}
    	}
    	else if ( table_type.equals("inner") ) {
    		if ( join_attr != -1 ) {
	    		proj = new FldSpec[proj_table.getTable_num_attr()-1];
	        	RelSpec rel = new RelSpec(RelSpec.innerRel);
	        	for ( int i=0; i<(join_attr - 1); i++ ) {
	        		proj[i] = new FldSpec(rel, i+1);
	        	}
	        	for ( int i=join_attr; i<proj_table.getTable_num_attr(); i++ ) {
	        		proj[i-1] = new FldSpec(rel, i+1);
	        	}
    		}
    		else {
    			proj = new FldSpec[proj_table.getTable_num_attr()];
            	RelSpec rel = new RelSpec(RelSpec.outer);
            	for ( int i=0; i<proj_table.getTable_num_attr(); i++ ) {
            		proj[i] = new FldSpec(rel, i+1);
            	}
    		}
    	}
    	else {
    		proj = null;
    	}
    	
    	return proj;
    }
    
    private FldSpec[] get_projection_for_join_table( FldSpec[] outer_proj, FldSpec[] inner_proj ) {
    	FldSpec[] join_proj = new FldSpec[outer_proj.length + inner_proj.length];
    	System.arraycopy(outer_proj, 0, join_proj, 0, outer_proj.length);
    	System.arraycopy(inner_proj, 0, join_proj, outer_proj.length, inner_proj.length);
    	return join_proj;
    }
    
    /* parses the join query for the exact structure 
     * part of task
     * structure: topkjoin HASH/NRA K OTABLENAME O_J_ATT_NO O_M_ATT_NO ITABLENAME I_J_ATT_NO I_M_ATT_NO NPAGES [MATER OUTTABLENAME]
     * */
    public void parse_topkjoin() throws JoinsException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
//    	try {
	    	/* ----------------------which join needs to be calculated ------------------------------------*/
	    	String join_algo = tokens[1]; //HASH/NRA
	    	
	    	/* ---------------------read K ------------------------------------*/
	    	int join_k = Integer.parseInt(tokens[2]); //K
	    	
	    	/* ---------------------which is the outer table----------------------------------*/
	    	String outer_table_name = tokens[3];
	    	
	    	/*----------------------get the outer nad inner attribute on which to perform the join */
	    	int outer_join_attribute = Integer.parseInt(tokens[4]);
	    	int outer_merge_attribute = Integer.parseInt(tokens[5]);
	    	
	    	/* ---------------------which is the inner table----------------------------------*/
	    	String inner_table_name = tokens[6];
	    	
	    	/*----------------------get the outer nad inner attribute on which to perform the join */
	    	int innerr_join_attribute = Integer.parseInt(tokens[7]);
	    	int inner_merge_attribute = Integer.parseInt(tokens[8]);
	    	
	    	/*------------join n_pages----------------------------*/
	    	int join_n_pages = Integer.parseInt(tokens[9]);
	    	SystemDefs.JavabaseBM.limit_memory_usage(true, join_n_pages);
	    	PCounter.initialize();
	    	
	    	/*---------------------extract tablename and outtablename--------------------------*/
	    	boolean is_output_saved = query.contains("MATER");
	    	String out_tablename = "";
	    	if ( is_output_saved ) {
	    		out_tablename = tokens[11];
	    	}
	    	
	    	if ( is_output_saved ) {
	    		System.out.println("Calculating top-K-join "+join_algo+" on outer table "+ outer_table_name+" join attribute "+ outer_join_attribute +
	    				" merge attribute "+ outer_merge_attribute +" and inner table "+ inner_table_name+" join attribute "+ innerr_join_attribute +
	    				" merge attribute "+ inner_merge_attribute +" with buffer pages: "+join_n_pages+
	    				". Saving the output table to "+out_tablename+". K = "+join_k);
	    	}
	    	else {
	    		System.out.println("Calculating top-K-join "+join_algo+" on outer table "+ outer_table_name+" join attribute "+ outer_join_attribute +
	    				" merge attribute "+ outer_merge_attribute +" and inner table "+ inner_table_name+" join attribute "+ innerr_join_attribute +
	    				" merge attribute "+ inner_merge_attribute +" with buffer pages: "+join_n_pages+". K = "+join_k);
	    	}
	    	
	    	/* run the appropriate skyline algorithm */
	    	switch ( join_algo ) {
	    		case "HASH":
	    			//TBD run top-K-join HASH with proper params
	    			FldSpec joinAttr1_Hash = new FldSpec(new RelSpec(RelSpec.outer), outer_join_attribute);
	    	        FldSpec mergeAttr1_Hash = new FldSpec(new RelSpec(RelSpec.outer), outer_merge_attribute);
	    	        
	    	        FldSpec joinAttr2_Hash = new FldSpec(new RelSpec(RelSpec.innerRel), innerr_join_attribute);
	    	        FldSpec mergeAttr2_Hash = new FldSpec(new RelSpec(RelSpec.innerRel), inner_merge_attribute);

	    	        Table t1hash = SystemDefs.JavabaseDB.get_relation(outer_table_name);
	        		Table t2hash = SystemDefs.JavabaseDB.get_relation(inner_table_name); 
	        		

				
	                TopK_HashJoin tjhj = new TopK_HashJoin(
	                		t1hash.getTable_attr_type(), t1hash.getTable_attr_type().length, t1hash.getTable_attr_size(),
	    	        		joinAttr1_Hash,
	    	    			mergeAttr1_Hash,
	                		t2hash.getTable_attr_type(), t2hash.getTable_attr_type().length, t2hash.getTable_attr_size(),
	    	    			joinAttr2_Hash,
	    	    			mergeAttr2_Hash,
	    	    			outer_table_name,
	    	    			inner_table_name,
	    	    			join_k,
						    join_n_pages
	    	    		);
	        		
	                Tuple t = tjhj.get_next();
	                
	                System.out.println("\nResult of Top-K HASH Join -->");
	                while(t != null) {
	                	t.print(tjhj.newAttrType);
	                	t = tjhj.get_next();
	                }
//	                System.out.println("ENDS HERE -----");
	                tjhj.close();
	    			break;
	    		case "NRA":
	    			//TBD run top-K-join NRA with proper params
	    			FldSpec joinAttr1 = new FldSpec(new RelSpec(RelSpec.outer), outer_join_attribute);
	    	        FldSpec mergeAttr1 = new FldSpec(new RelSpec(RelSpec.outer), outer_merge_attribute);
	    	        
	    	        FldSpec joinAttr2 = new FldSpec(new RelSpec(RelSpec.innerRel), innerr_join_attribute);
	    	        FldSpec mergeAttr2 = new FldSpec(new RelSpec(RelSpec.innerRel), inner_merge_attribute);
	    	        
	    	        Table t1 = SystemDefs.JavabaseDB.get_relation(outer_table_name);
	        		Table t2 = SystemDefs.JavabaseDB.get_relation(inner_table_name); 
	        		
	
	    			TopK_NRAJoin tknj = new TopK_NRAJoin(
	    					t1.getTable_attr_type(), t1.getTable_attr_type().length, t1.getTable_attr_size(),
					        joinAttr1,
					        mergeAttr1,
	    					t2.getTable_attr_type(), t2.getTable_attr_type().length, t2.getTable_attr_size(),
					        joinAttr2,
					        mergeAttr2,
					        outer_table_name,
					        inner_table_name,
					        join_k, 
					        join_n_pages
	    			);
			
					try {
						tknj.calculateTopKJoins();
					} catch (Exception e) {
						e.printStackTrace();
					}
					
					Tuple r = tknj.get_next();
					
					System.out.println("\nResult of Top-K NRA Join -->");
					while(r != null) {
						r.print(tknj.joinAttrType);
						r = tknj.get_next();
					}
					tknj.close();
	    			break;
	    		default:
	    			validate_token_length(0, "topkjoin");
	    			break;
	    	}
	    	/*printing the reads and writes and closing pcounter and also free the BM from the limit */
	    	System.out.println("\nNumber of Page reads: "+PCounter.get_rcounter());
	    	System.out.println("Number of Page Writes: "+PCounter.get_wcounter());
	    	SystemDefs.JavabaseBM.limit_memory_usage(false, join_n_pages);
	    	PCounter.initialize();
    }
    
    public void parse_table_info() {
    	String tablename = tokens[1];
    	System.out.println("Printing the table "+tablename);
    	try {
			Table table = SystemDefs.JavabaseDB.get_relation(tablename);
			if ( table == null ) {
				System.out.println("*********ERROR: table does not exist **************");
			}
			else {
				table.print_table_attr();
			}
		} catch (InvalidTupleSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public void run_test_query() {
    	/*try {
			Heapfile hp = new Heapfile("blah.in");
			AttrType[] attrType = new AttrType[1];
			short[] strsizes = new short[1];
			strsizes[0] = 100;
			attrType[0] = new AttrType(AttrType.attrInteger);
			Tuple t = TupleUtils.getEmptyTuple(attrType, strsizes);
			RID wer = new RID();
			for ( int i = 0; i<2000; i++ ) {
				
				hp.insertRecord(t.getTupleByteArray());
			}
			Scan sc = hp.openScan();
			Tuple t1;
			t1 = sc.getNext(wer);
			while ( t1 != null ) {
				t.tupleCopy(t1);
				t1 = sc.getNext(wer);
				t.print(attrType);
			}
			System.out.println(hp.getRecCnt());
			sc.closescan();
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
		} catch (InvalidTypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTupleSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidSlotNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SpaceNotAvailableException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
    	Table table = SystemDefs.JavabaseDB.get_relation("r_sii2000_1_75_200");
    	try {
			table.test();
		} catch (InvalidTypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTupleSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    /* This function runs the main query interface which reads and processes all the queries */
    public void startQueryInterface() throws Exception {
    	boolean exit_interface = false;
    	while (!exit_interface) {
    		query = readNextCommand();
    		tokens = query.split(" ");
    		switch (tokens[0]) {
    			case "exit":
    				// TBD need to close the DB and save the open stuff here
    				handleOpenDB(true);
    				System.out.println("exiting query interface");
    				exit_interface = true;
    				break;
    			case "destroy_database":
    				destroy_db();
    				break;
    			case "open_database":
    				/* open new or already existing database 
    				 * open_database dbname
    				 */
    				handleOpenDB(false);
    				break;
    			case "create_table":
    				/* need to create a simple or a clustered index table here
    				 * create_table [CLUSTERED BTREE/HASH ATT_NO] FILENAME
    				 */
    				parse_create_table();
    				break;
    			case "close_database":
    				/*need to close the existing db and dump all the file on the disk
    				 * close_database
    				 */
    				handleOpenDB(true);
    				break;
    			case "create_index":
    				/*need to create an unclustered index as specified
    				 * create_index BTREE/HASH ATT_NO TABLENAME
    				 */
    				parse_create_index();
    				break;
    			case "insert_data":
    				/*insert data to the file
    				 * insert_data TABLENAME FILENAME
    				 */
    				parse_insert_data();
    				break;
    			case "delete_data":
    				/*delete the elements from the given file
    				 * delete_data TABLENAME FILENAME 
    				 */
    				parse_delete_data();
    				break;
    			case "output_table":
    				/*print the entire table specified
    				 * output_table TABLENAME
    				 */
    				parse_output_table();
    				break;
    			case "output_index":
    				/*outputs the index if available
    				 * output_index TABLENAME ATT_NO
    				 */
    				parse_output_index();
    				break;
    			case "skyline":
    				/*computes the skyline mentioned
    				 * skyline NLS/BNLS/SFS/BTS/BTSS {ATT_NO1, ...ATT_NOh} TABLENAME NPAGES [MATER OUTTABLENAME]
    				 */
    				parse_skyline();
    				break;
    			case "groupby":
    				/*calculate the group by accordingly
    				 * groupby SORT/HASH MAX/MIN/AGG/SKY G_ATT_NO{ATT_NO1,...ATT_NOh} TABLENAME NPAGES [MATER OUTTABLENAME]
    				 */
    				parse_groupby();
    				break;
    			case "join":
    				/*calculate the join as specified
    				 * join NLJ/SMJ/INLJ/HJ OTABLENAME O_ATT_NO ITABLENAME I_ATT_NO OP NPAGES [MATER OUTTABLENAME]
    				 */
    				parse_join();
    				break;
    			case "topkjoin":
    				/*perform the topkjoin as specified
    				 * topkjoin HASH/NRA K OTABLENAME O_J_ATT_NO O_M_ATT_NO ITABLENAME I_J_ATT_NO I_M_ATT_NO NPAGES [MATER OUTTABLENAME]
    				 */
    				parse_topkjoin();
    				break;
    			case "help":
    				printQueryHelper("all");
    				break;
    			case "test":
    				run_test_query();
    				break;
    			case "output_table_info":
    				parse_table_info();
    				break;
    			default:
    				System.out.println("Query command not recognized "+tokens[0]);
    				printQueryHelper("all");
    				break;
    		}
    	}
    }
    
    /* This function will print all the available functions and their structures incase a wrong query is entered */
    public void printQueryHelper(String s) {
    	switch (s) {
    		case "open_database":
    			System.out.println("Command: open_database dbname--> opens a database with the given name. If the db does not exist, reates the db from scratch\n\n");
    			break;
    		case "close_database":
    			System.out.println("Command: close_database--> Closes the current database and make all the files in the db persistent\n\n");
    			break;
    		case "create_table":
    			System.out.println("Command: create_table [CLUSTERED BTREE/HASH ATT_NO] FILENAME--> Create a new table and populte it with data from the given file"
    					+ ", with the following format");
    			System.out.println("\t\t1. First row of the file contains the number, n, of attributes");
    			System.out.println("\t\t2. The next rows containshe attribute names and types (INT/STR)");
    			System.out.println("\t\t3. The rest of the rows contains the data tuples");
    			System.out.println("If CLUSTERED BTREE or CLUSTERED HASH is specified, then create a  clustered index on the file on the attribute specified"
    					+ " by ATT_NO ( first attribute at index 1 and second at indexx 2 and so on.\n\n");
    			break;
    		case "create_index":
    			System.out.println("Command: create_index BTREE/HASH ATT_NO TABLENAME--> Create and unclustered index on the file on the attribute specified by"
    					+ " ATT_NO. If an index already exists, then the operation returns without any index creation. once the index is created, it needs to"
    					+ " be maintained with the insertions and deletions\n\n");
    			break;
    		case "insert_data":
    			System.out.println("Command: insert_data TABLENAME FILENAME--> Insert data to the given table from the given file\n\n");
    			break;
    		case "delete_data":
    			System.out.println("Command: delete_data TABLENAME FILENAME--> Delete data from the given table those data that appear in the give file.\n\n");
    			break;
    		case "output_table":
    			System.out.println("Command: output_table TABLENAME--> Output all the tuples in the given table.\n\n");
    			break;
    		case "output_index":
    			System.out.println("Command: output_index TABLENAME ATT_NO--> Output the keys in the (clustered or unclustered) index of the table for the given"
    					+ " attribute. If there is no index at the given attribute, then the operation outputs N/A.\n\n");
    			break;
    		case "skyline":
    			System.out.println("Command: skyline NLS/BNLS/SFS/BTS/BTSS {ATT_NO1, ...ATT_NOh} TABLENAME NPAGES [MATER OUTTABLENAME]--> Output the skyline"
    					+ " of the given table for the given h attributes. If MATER is specified, then materialize the results by creating a new table with "
    					+ " specified outputtable name.\n\n");
    			break;
    		case "groupby":
    			System.out.println("Command: GROUPBY SORT/HASH MAX/MIN/AGG/SKY G_ATT_NO{ATT_NO1,...ATT_NOh} TABLENAME NPAGES [MATER OUTTABLENAME]--> Output the "
    					+ " result of the groupby/aggregation operation. G_ATT_NO specifies the groupby attributes. ATT_NO1 through ATT_NOh  specify the"
    					+ " aggregation attributes. If MATER is specified, then materialize the results by creating a new table with the specified output table name\n\n");
    			break;
    		case "join":
    			System.out.println("Command: JOIN NLJ/SMJ/INLJ/HJ OTABLENAME O_ATT_NO ITABLENAME I_ATT_NO OP NPAGES [MATER OUTTABLENAME]--> Output the "
    					+ " result of specified join operation on the given out and inner relations. The join condition is specified by two given attributes and "
    					+ "operator OP which belongs to  {=, <=, <, >, >=}. If MATER is specified, then materialize the results by creating a new table with"
    					+ " the specified output table name.\n\n");
    			break;
    		case "topkjoin":
    			System.out.println("Command: TOPKJOIN HASH/NRA K OTABLENAME O_J_ATT_NO O_M_ATT_NO ITABLENAME I_J_ATT_NO I_M_ATT_NO NPAGES [MATER OUTTABLENAME"
    					+ "]--> Output the result of the specified top-K-join operation on the given out and inner relations. The join condtion  is specified by "
    					+ "the two given attributes and the selection of the top-K results are performed based in the specified merge attributes. If MATER is"
    					+ " specified, then materialize the results by creating a new table with the specified output table name.\n\n");
    			break;
    			
    		case "all":
    			printQueryHelper("open_database");
    			printQueryHelper("close_database");
    			printQueryHelper("create_table");
    			printQueryHelper("create_index");
    			printQueryHelper("insert_data");
    			printQueryHelper("delete_data");
    			printQueryHelper("output_table");
    			printQueryHelper("output_index");
    			printQueryHelper("skyline");
    			printQueryHelper("groupby");
    			printQueryHelper("join");
    			printQueryHelper("topkjoin");
    			break;
    	}
    }
    
    /* This function is used to read and parse the next command in the query interface */
    public String readNextCommand() {
    	System.out.print(">>");
    	Scanner sc = new Scanner(System.in);
    	String input_command = sc.nextLine();
    	return input_command;
    }
    
    public boolean runTests () {
        System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");
        // Each page can handle at most 25 tuples on original data => 7308 / 25 = 292

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

        // Commands here is very machine dependent.  We assume
        // user are on UNIX system here
        /*try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }*/

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        /*try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }*/

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();

        //Clean up again
        /*try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }*/

        System.out.print ("\n" + "..." + testName() + " tests ");
        System.out.print (_pass==OK ? "completely successfully" : "failed");
        System.out.print (".\n\n");
        return _pass;
    }

    private void menu() {
        System.out.println("------------SKYLINE PROCESSING MENU ------------------");
        System.out.println("[101]   Set pref = [1]");
        System.out.println("[102]   Set pref = [1,3]");
        System.out.println("[103]   Set pref = [1,2]");
        System.out.println("[104]   Set pref = [1,3,5]");
        System.out.println("[105]   Set pref = [1,2,3,4,5]");
        System.out.println("[106]   Set n_page = 5");
        System.out.println("[107]   Set n_page = 10");
        System.out.println("[108]   Set n_page = <your_wish>");
        System.out.println("[1]  Run Nested Loop skyline on data with parameters ");
        System.out.println("[2]  Run Block Nested Loop on data with parameters ");
        System.out.println("[3]  Run Sort First Sky on data with parameters ");
        System.out.println("[4]  Run Btree Sky on data with parameters ");
        System.out.println("[5]  Run Btree Sort Sky on data with parameters ");
        System.out.println("\n[0]  Quit!");
        System.out.print("Hi, make your choice :");
    }
    
    private void dbcreationmenu() {
    	 System.out.println("------------------DB CREATION MENU ------------------");
         System.out.println("[1]   Read input data data2.txt");
         System.out.println("[2]   Read input data data3.txt");
         System.out.println("[3]   Read input data data_large_skyline.txt");
         System.out.print("Hi, make your choice :");

        String OS = System.getProperty("os.name").toLowerCase();

        int choice= GetStuffPhase3.getChoice();
         switch(choice) {
         case 1:
        	 dataFile = OS.indexOf("mac") >= 0 ? "data/demo_data/nc_2_3000_single.txt" : "data/data2.txt";
        	 numberOfDimensions = 2;
        	 break;
         case 2:
        	 dataFile = OS.indexOf("mac") >= 0 ? "data/data3.txt" : "data/data3.txt";
        	 numberOfDimensions = 5;
        	 break;
         case 3:
        	 dataFile = OS.indexOf("mac") >= 0 ? "data/data_large_skyline.txt" : "data/data_large_skyline.txt";
        	 numberOfDimensions = 2;
        	 break;
         default:
        	 System.err.println("Invalid Choice");
        	 System.exit(-1);
        	 break;        	 
         }
		try {
			System.out.println("Reading file: "+dataFile);
			readDataIntoHeap(dataFile);
			BtreeGeneratorUtil.generateAllBtreesForHeapfile(hFile, f, attrType, attrSize);
			individualBTreeIndexesCreated = true;
			System.out.println("DATABASE CREATED");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
    }
    
    private void readDataIntoHeap(String fileName) throws IOException, InvalidTupleSizeException, InvalidTypeException, InvalidSlotNumberException, HFDiskMgrException, HFBufMgrException, HFException, HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException {

        // Create the heap file object
        try {
            f = new Heapfile(hFile);
        }
		catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
			status = FAIL;
			System.err.println("*** Could not create heap file\n");
			e.printStackTrace();
			throw e;
		}
       

        if ( status == OK ) {

            // Read data and construct tuples

            File file = new File(fileName);
            Scanner sc = new Scanner(file);

            COLS = sc.nextInt();
            sc.nextLine(); // skipping the whole first line from the file as that has only 5 in it

            attrType = new AttrType[COLS];
            attrSize = new short[COLS];

            for(int i=0; i<attrType.length; i++){
                attrType[i] = new AttrType(AttrType.attrReal);
            }

            for(int i=0; i<attrSize.length; i++){
                attrSize[i] = 32;
            }

            projlist = new FldSpec[COLS];

            for(int i=0; i<attrType.length; i++){
                projlist[i] = new FldSpec(rel, i+1);;
            }

            Tuple t = new Tuple();
            try {
                t.setHdr((short) COLS,attrType, attrSize);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            int size = t.size();
            _t_size = t.size();
            //System.out.println("Size: "+size);

            t = new Tuple(size);
            try {
                t.setHdr((short) COLS, attrType, attrSize);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            while (sc.hasNextLine()) {
                // create a tuple of appropriate size

                double[] doubleArray = Arrays.stream(Arrays.stream(sc.nextLine().trim()
                        .split("\\s+"))
                        .filter(s -> !s.isEmpty())
                        .toArray(String[]::new))
                        .mapToDouble(Double::parseDouble)
                        .toArray();
                
                for(int i=0; i<doubleArray.length; i++) {
                    try {
                        t.setFloFld(i+1, (float) doubleArray[i]);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }

                try {
                    rid = f.insertRecord(t.returnTupleByteArray());
                }
                catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                //System.out.println("RID: "+rid);
            }
            System.out.println("Number of records in Database: "+f.getRecCnt());
            sc.close();
        }
       
		SystemDefs.JavabaseBM.flushAllPages();
		
    }

    protected String testName () {
        return "Main Driver";
    }

	protected boolean runAllTests (){
        int choice=100;
        
        dbcreationmenu();
       
        while(choice!=0) {
            menu();
           
            try{
            	
                choice= GetStuffPhase3.getChoice();

                switch(choice) {
                    case 101:
                        _pref_list = new int[]{1};
                        break;

                    case 102:
                        _pref_list = new int[]{1,3};
                        break;
                    case 103:
                        _pref_list = new int[]{1,2};
                        break;
                    case 104:
                        _pref_list = new int[]{1,3,5};
                        break;

                    case 105:
                        _pref_list = new int[]{1,2,3,4,5};
                        break;

                    case 106:
                        _n_pages = 5;
                        System.out.println("n_pages set to :" + _n_pages);
                        break;

                    case 107:
                        _n_pages = 10;
                        System.out.println("n_pages set to :" + _n_pages);
                        break;

                    case 108:
                        System.out.println("Enter n_pages of your choice: ");
                        _n_pages = GetStuffPhase3.getChoice();
                        if(_n_pages<0)
                            break;
                        System.out.println("n_pages set to :" + _n_pages);
                        break;

                    case 1:
                        // call nested loop sky
                        //runNestedLoopSky();
                        break;

                    case 2:
                        // call block nested loop sky
                        //blockNestedSky();
                        break;

                    case 3:
                        // call sort first sky
                        //runSortFirstSky();
                        break;

                    case 4:
                        // call btree sky
                    	//runBtreeSky();
                        break;

                    case 5:
                        // call btree sort sky
                        //runBtreeSortSky();
                        break;

                    case 0:
                    	SystemDefs.JavabaseDB.DBDestroy();
                        break;
                }


            }
            catch (Exception e) {
            	//checking for buffer full exception, then dont print full stack trace
            	boolean caught = false;
            	if (e instanceof ChainException) {
            		ChainException temp = (ChainException) e;
            		while (caught == false && temp != null) {
            			if (temp instanceof bufmgr.BufferPoolExceededException) {
            				caught = true;
            				System.err.println(temp.getMessage());
            				System.err.println(
            						"BufferPoolExceededException, insufficient buffer memory for this operation ");
            			}
            			System.err.println(temp.getMessage());
            			temp = (ChainException) temp.prev;
            		}
            	}

            	if (!caught) {
            		System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            		System.out.println("       !!         Something is wrong                    !!");
            		System.out.println("       !!     Is your DB full? then exit. rerun it!     !!");
            		System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            		e.printStackTrace();
            	}
            }
        }

        return true;
    }
	
	/*private void runBtreeSortSky() throws Exception {
        System.out.println("Will run btree sort sky with params: ");
        System.out.println("N pages: "+_n_pages);

        int [] pref_list = new int[numberOfDimensions];
        
        for(int i = 0; i < _pref_list.length; i++) {
        	pref_list[ _pref_list[i] - 1 ] = 1;
        }
        
        for(int i = 0; i < numberOfDimensions; i++) {
        	if(pref_list[i] != 1) pref_list[i] = 0;
        }
        
        System.out.println("Pref list: "+Arrays.toString(pref_list));
        System.out.println("Pref list length: "+numberOfDimensions);

        //limiting buffer pages in BufMgr
        System.out.println("No of buffers "+SystemDefs.JavabaseBM.getNumBuffers());
        System.out.println("No of unpinned buffers "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers());

        SystemDefs.JavabaseBM.limit_memory_usage(true, this._n_pages);


        GenerateIndexFiles obj = new GenerateIndexFiles();
        IndexFile indexFile = obj.createCombinedBTreeIndex(dataFile,,pref_list, pref_list.length);
        System.out.println("Index created! ");
        Tuple t = new Tuple();
        short [] Ssizes = null;

        AttrType [] attrType = new AttrType[pref_list.length];
        for(int i=0;i<pref_list.length;i++){
            attrType[i] = new AttrType (AttrType.attrReal);
        }

        t.setHdr((short)pref_list.length, attrType, Ssizes);
        int size = t.size();

        t = new Tuple(size);
        t.setHdr((short)pref_list.length, attrType, Ssizes);

        PCounter.initialize();
        int numSkyEle = 0;
        BTreeSortedSky btree = new BTreeSortedSky(attrType, pref_list.length, Ssizes, 0, null, "heap_AAA", _pref_list, _pref_list.length, indexFile, _n_pages );
        btree.computeSkylines();

        System.out.println("Printing the Btree sorted Skyline");
        Tuple temp;
        temp = btree.get_next();
        while (temp!=null) {
        	temp.print(attrType);
        	numSkyEle++;
        	temp = btree.get_next();
        }
        System.out.println("Skyline Length: "+numSkyEle);
        System.out.println("Number of Disk reads: "+ PCounter.get_rcounter());
        System.out.println("Number of Disk writes: "+ PCounter.get_wcounter());
        PCounter.initialize();
        btree.close();
        
    }*/

}


/**
 * To get the integer off the command line
 */
class GetStuffPhase3 {
	GetStuffPhase3() {}

    public static int getChoice () {
        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        int choice = -1;

        try {
            choice = Integer.parseInt(in.readLine());
        }
        catch (NumberFormatException e) {
            return -1;
        }
        catch (IOException e) {
            return -1;
        }

        return choice;
    }

    public static void getReturn () {

        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));

        try {
            String ret = in.readLine();
        }
        catch (IOException e) {}
    }
}

public class AppDriverPhase3 implements  GlobalConst{

    public static void main(String [] argvs) {

        try{
        	DriverPhase3 driver = new DriverPhase3();
            driver.startQueryInterface();
        }
        catch (Exception e) {
            System.err.println ("Error encountered during running main driver:\n");
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }finally {

        }
    }

}