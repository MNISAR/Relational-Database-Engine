package iterator;

import heap.*;
import index.IndexException;
import global.*;
import bufmgr.*;
import diskmgr.PCounter;
import diskmgr.Page;

import java.lang.*;
import java.io.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;

import btree.KeyDataEntry;
import btree.ScanIteratorException;

/**
 *use the iterator and relationName to compute the skyline using nested loop method
 *output file, call get_next to get all tuples
 */
public class BlockNestedLoopsSky extends Iterator implements GlobalConst
{
	/* Tuple attributes array */
	private AttrType[]   _in1;

	/* number of attributes for any tuple */
	private short        _len_in1;

	/* length of the string fields in the tuple */
	private short[]      _t1_str_sizes;

	/* filename for the file containing tuples */
	private String       _relation_name;

	/* name of the temporary heap file created to store skyline elements */
	private String       _temp_heap_file_name;

	/* temporary heap file used to store the skyline elements */
	private Heapfile     _temp_heap_file;

	/* scan on the temporary heap file used to compute the skyline */
	private Scan         _scan;

	/* indices of attributes of a tuple to be considered for the skyline */
	private int[]        _pref_list;

	/* number of attributes of a tuple to be considered for the skyline */
	private int          _pref_list_length;

	/* number of pages available for the skyline operation */
	private int          _n_pages;

	/* heap file containing our data on which skyline is computed */
	private Heapfile     _heap_file;

	/* outer scan on the heapfile containing the data */
	private Scan         _outer_scan;

	/* stores the status of each operation */
	private boolean      _status;

	/* Windows which is used to store some skyline elements */
	private Queue<Tuple> _queue;

	/* number of tuples the queue can hold */
	private int          _window_size;

	/* tuples used for computation during the skyline -- for debug purposes */
	private Tuple        outer_candidate_temp, inner_candidate_temp, outer_candidate, inner_candidate;

	/* temporary record ID used to calculate skyline -- for debug purposes */
	private RID          _temp;

	/* size of the tuples in the skyline/window/temporary_heap_file */
	private int          _tuple_size;

	/* keeps track of number of skyline elements returned from the window */
	private int          _counter;

	/**
	 *constructor
	 *@param in1  array showing what the attributes of the input fields are.
	 *@param len_in1  number of attributes in the input tuple
	 *@param t1_str_sizes  shows the length of the string fields
	 *@param am1 iterator over the data file ( not used in our case; passing it as null )
	 *@param relationName heapfile to be opened
	 *@param pref_list array of the indices of the preferred attributes
	 *@param pref_list_length number of preferred attributes
	 *@param n_pages number of pages available for the skyline operation
	 *@exception IOException some I/O fault
	 *@exception FileScanException exception from this class
	 *@exception TupleUtilsException exception from this class
	 *@exception InvalidRelation invalid relation
	 */
	public  BlockNestedLoopsSky
	(
			AttrType[] in1,
			int len_in1,
			short[] t1_str_sizes,
			Iterator am1,
			String relationName,
			int[] pref_list,
			int pref_list_length,
			int n_pages
	)
			throws	IOException,
			FileScanException,
			TupleUtilsException,
			InvalidRelation
	{
		/* initialize all variables */
		this._in1 = in1;
		this._len_in1 = (short)len_in1;
		this._t1_str_sizes = t1_str_sizes;
		this._relation_name = relationName;
		this._pref_list = pref_list;
		this._pref_list_length = pref_list_length;
		this._n_pages = n_pages;
		this._scan = null;
		this._temp = new RID();
		this._temp_heap_file_name = System.currentTimeMillis() + "temp_block_nested_loop.in";
		this._queue = new LinkedList<>();
		this._counter = 0;
		int buffer_pages = n_pages;
		/* In this algorithm we will be allocating n_pages/2 pages to the buffer manager and rest
		 * to the window used to store tuples in main memory.
		 */
		try {
			/* limit the memory usage of BM to calculated pages */
			buffer_pages = this._n_pages/2;
//        	SystemDefs.JavabaseBM.limit_memory_usage(true, buffer_pages);
			this._heap_file = new Heapfile(this._relation_name);
//        	System.out.println("BNL heapfile size "+_heap_file.getRecCnt());
			this._temp_heap_file = new Heapfile(this._temp_heap_file_name);
			this._status = true;
		}
		catch (Exception e) {
			System.err.println("Could not open the heapfile");
			e.printStackTrace();
		}

		/* open a scan on the data file */
		if ( this._status == true )
		{
			try
			{
				this._outer_scan = this._heap_file.openScan();
				this._status = true;
			}
			catch (Exception e)
			{
				this._status = false;
				System.err.println ("*** Error opening scan\n");
				e.printStackTrace();
			}
		}

		/* initialise tuple size */
		try {
			this.outer_candidate_temp = new Tuple();
			this.outer_candidate_temp.setHdr(this._len_in1, this._in1, this._t1_str_sizes);
			this._tuple_size = this.outer_candidate_temp.size();
		} catch (InvalidTypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTupleSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/* calculate the window size and start computing the skyline */
		try {
			this._window_size = 5;
//        	System.out.println("Number of pages reserved for the window are "+ (this._n_pages - buffer_pages) );
			compute_skyline();
		} catch (JoinsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTupleSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PageNotReadException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PredEvalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknowAttrType e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FieldNumberOutOfBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (WrongPermat e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TupleUtilsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


	/**
	 *@return the result tuple
	 *@exception JoinsException some join exception
	 *@exception IOException I/O errors
	 *@exception InvalidTupleSizeException invalid tuple size
	 *@exception InvalidTypeException tuple type not valid
	 *@exception PageNotReadException exception from lower layer
	 *@exception PredEvalException exception from PredEval class
	 *@exception UnknowAttrType attribute type unknown
	 *@exception FieldNumberOutOfBoundException array out of bounds
	 *@exception WrongPermat exception for wrong FldSpec argument
	 */
	public void compute_skyline()
			throws JoinsException,
			IOException,
			InvalidTupleSizeException,
			InvalidTypeException,
			PageNotReadException,
			PredEvalException,
			UnknowAttrType,
			FieldNumberOutOfBoundException,
			WrongPermat, TupleUtilsException
	{
		/* read the next tuple from outer_iterator
		 * Assumption here is that inner iterator is a filescan iterator */
		this.outer_candidate = new Tuple(this._tuple_size);
		outer_candidate.setHdr(this._len_in1, this._in1, this._t1_str_sizes);
		this.inner_candidate = new Tuple(this._tuple_size);
		inner_candidate.setHdr(this._len_in1, this._in1, this._t1_str_sizes);
		while (true)
		{
			outer_candidate_temp = this._outer_scan.getNext(_temp);
			if (outer_candidate_temp == null)
			{
				this._outer_scan.closescan();
				break;
			}
			outer_candidate.tupleCopy(outer_candidate_temp);
			/* open a scan on the heapfile/relationname for inner loop */
			if ( this._status == true )
			{
				try
				{
					this._scan = this._temp_heap_file.openScan();
				}
				catch (Exception e)
				{
					this._status = false;
					System.err.println ("*** Error opening scan\n");
					e.printStackTrace();
				}
			}
			if ( can_be_added_to_queue(outer_candidate) && can_be_added_to_heap(outer_candidate) )
			{
				Tuple temp_adder = new Tuple(this._tuple_size);
				temp_adder.setHdr(_len_in1, _in1, _t1_str_sizes);
				temp_adder.tupleCopy(outer_candidate);
				if ( this._queue.size() < this._window_size )
				{
					this._queue.add(temp_adder);
				}
				else
				{
					try {
						//System.out.println("Inserting tuple to heap");
						_temp = this._temp_heap_file.insertRecord(temp_adder.returnTupleByteArray());
					} catch (InvalidSlotNumberException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InvalidTupleSizeException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (SpaceNotAvailableException e) {
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
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			this._scan.closescan();
		}
		this._scan = this._temp_heap_file.openScan();
	}

	public boolean can_be_added_to_queue(Tuple candidate)
	{
		int temp_counter = 0;
		while ( ( temp_counter < this._queue.size() ) && ( !this._queue.isEmpty() ) )
		{
			//System.out.println("while2");
			inner_candidate_temp = this._queue.remove();
			//System.out.println("inner candidate temp: ");

			try {
				//System.out.println("queue dominates candidate check");
				//this.inner_candidate_temp.print(_in1);
				//candidate.print(_in1);
				boolean queue_dominates_candidate = TupleUtils.Dominates(this.inner_candidate_temp,
						this._in1,
						candidate,
						this._in1,
						this._len_in1,
						this._t1_str_sizes,
						this._pref_list,
						this._pref_list_length);
				if ( queue_dominates_candidate )
				{
					this._queue.add(inner_candidate_temp);
					//System.out.println("queue dominates the candidate");
					return false;
				}
			} catch (TupleUtilsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnknowAttrType e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (FieldNumberOutOfBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				boolean candidate_dominates_queue = TupleUtils.Dominates(candidate,
						this._in1,
						this.inner_candidate_temp,
						this._in1,
						this._len_in1,
						this._t1_str_sizes,
						this._pref_list,
						this._pref_list_length);
				if ( candidate_dominates_queue )
				{
					// we need to delete the queue element
					//do nothing because we dont want to increase the counter and dont want to add the element back to the queue either
					//System.out.println("Candidate dominated the queue");
				}
				else
				{
					//System.out.println("Candidate made no difference in the queue");
					temp_counter++;
					this._queue.add(inner_candidate_temp);
				}
			} catch (TupleUtilsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnknowAttrType e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (FieldNumberOutOfBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		return true;
	}

	public boolean can_be_added_to_heap(Tuple candidate)
	{
		try {
			inner_candidate_temp = this._scan.getNext(_temp);
		} catch (InvalidTupleSizeException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		while ( inner_candidate_temp != null )
		{
			//System.out.println("while3");
			this.inner_candidate.tupleCopy(inner_candidate_temp);
			try {
				boolean heap_dominates_candidate = TupleUtils.Dominates(this.inner_candidate,
						this._in1,
						candidate,
						this._in1,
						this._len_in1,
						this._t1_str_sizes,
						this._pref_list,
						this._pref_list_length);
				if ( heap_dominates_candidate )
				{
					return false;
				}
			} catch (TupleUtilsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnknowAttrType e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (FieldNumberOutOfBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				boolean candidate_dominates_heap = TupleUtils.Dominates( candidate,
						this._in1,
						this.inner_candidate,
						this._in1,
						this._len_in1,
						this._t1_str_sizes,
						this._pref_list,
						this._pref_list_length);
				if ( candidate_dominates_heap )
				{
					// we need to delete the heap element
					try {
						this._scan.closescan();
						this._temp_heap_file.deleteRecord(_temp);
						this._scan = this._temp_heap_file.openScan();
					} catch (InvalidSlotNumberException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InvalidTupleSizeException e) {
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
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} catch (TupleUtilsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnknowAttrType e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (FieldNumberOutOfBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				inner_candidate_temp = this._scan.getNext(_temp);
			} catch (InvalidTupleSizeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return true;
	}

	/**
	 *implement the abstract method close() from super class Iterator
	 *to finish cleaning up
	 */
	public void close()
	{
		if (!closeFlag)
		{
			closeFlag = true;
			this._scan.closescan();
			try {
				this._temp_heap_file.deleteFile();
			} catch (InvalidSlotNumberException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (FileAlreadyDeletedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTupleSizeException e) {
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
			}
		}

		// _scan.closescan();
		//_outer_scan.closescan();
	}


	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception
	{
		// TODO Auto-generated method stub
		if ( this._counter < this._queue.size() )
		{
			this._counter++;
			this.inner_candidate_temp = this._queue.remove();
			Tuple adder = new Tuple(this._tuple_size);
			adder.setHdr(_len_in1, _in1, _t1_str_sizes);
			adder.tupleCopy(this.inner_candidate_temp);
			this._queue.add(adder);
			return this.inner_candidate_temp;
		}
		else
		{
			this.inner_candidate_temp = this._scan.getNext(_temp);
			if ( this.inner_candidate_temp != null )
			{
				this.inner_candidate.tupleCopy(this.inner_candidate_temp);
				return this.inner_candidate;
			}
		}
//		SystemDefs.JavabaseBM.limit_memory_usage(false, this._n_pages);
//		System.out.println("No more records in skyline. All records already scanned.");
		return null;
	}


	@Override
	public List<Tuple> get_next_aggr() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public KeyDataEntry get_next_key_data() throws ScanIteratorException {
		// TODO Auto-generated method stub
		return null;
	}

}