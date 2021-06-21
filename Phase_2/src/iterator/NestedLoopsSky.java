package iterator;

import heap.*;
import global.*;
import bufmgr.*;

import java.lang.*;
import java.io.*;
import java.util.Arrays;

/**
 *use the iterator and relationName to compute the skyline using nested loop method
 *output file, call get_next to get next tuple in the skyline
 */
public class NestedLoopsSky extends Iterator
{
	/* Tuple attributes array */
    private AttrType[]  _in1;
    
    /* number of attributes for any tuple */
    private short        _len_in1;
    
    /* length of the string fields in the tuple */
    private short[]     _t1_str_sizes;
    
    /* filename for the file containing tuples */
    private String      _relation_name;
    
    /* indices of attributes of a tuple to be considered for the skyline */
    private int[]       _pref_list;
    
    /* number of attributes of a tuple to be considered for the skyline */
    private int         _pref_list_length;
    
    /* number of pages available for the skyline operation */
    private int         _n_pages;
    
    /* heap file containing our data on which skyline is computed */
    private Heapfile    _heap_file;
    
    /* stores the status of each operation */
    private boolean     _status;
    
    /* inner scan on the heapfile containing the data */
    private Scan        _inner_scan;
    
    /* outer scan on the heapfile containing the data */ 
    private Scan        _outer_scan;
    
    /* iterator over the data ( NULL in our implementation )*/
    private Iterator    _itr;
    
    /* tuples used for computation during the skyline -- for debug purposes */
    private Tuple       outer_candidate, inner_candidate, outer_candidate_temp, inner_candidate_temp;
    
    /* size of the tuples in the skyline */
    private int         _tuple_size;

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
    public  NestedLoopsSky
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
        this._in1 = in1;
        this._len_in1 = (short)len_in1;
        this._t1_str_sizes = t1_str_sizes;
        this._itr = am1;
        this._relation_name = relationName;
        this._pref_list = pref_list;
        this._pref_list_length = pref_list_length;
        this._n_pages = n_pages;
        this._inner_scan = null;
        SystemDefs.JavabaseBM.limit_memory_usage(true, this._n_pages);
        try {
        	/* open the data heap file */
        	this._heap_file = new Heapfile(this._relation_name);
        	/* open a scan on the heap file */
            this._outer_scan = this._heap_file.openScan();
            this._status = true;
        }
        catch (Exception e) {
            System.err.println("Could not open the heapfile");
            e.printStackTrace();
        }
        
        /* initialise the tuple size */
        try {
        	outer_candidate_temp = new Tuple();
			outer_candidate_temp.setHdr(this._len_in1, this._in1, this._t1_str_sizes);
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

    }



    /**
     *@return Tuple next tuple in the skyline
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
    public Tuple get_next()
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
    	/* ---------------------------------------------Algorithm ----------------------------------------------- 
    	 * step 1: set the size of the tuples to be compared and returned
    	 * step 2: on each iteration, start comparing the outer scan tuple with all the inner scan tuples
    	 * step 3: return the outer scan tuple if it is not dominated by any tuple in the inner scan
    	 * 
    	 * The function returns the next skyline element or null if skyline is over
    	 */
        
    	this.outer_candidate = new Tuple(this._tuple_size);
        outer_candidate.setHdr(this._len_in1, this._in1, this._t1_str_sizes);
        this.inner_candidate = new Tuple(this._tuple_size);
        inner_candidate.setHdr(this._len_in1, this._in1, this._t1_str_sizes);
        RID temp = new RID();
        while (true)
        {
            this.outer_candidate_temp = this._outer_scan.getNext(temp);
            if (this.outer_candidate_temp == null)
            {
                System.out.println("No more records in skyline. All records already scanned.");
                this._outer_scan.closescan();
                SystemDefs.JavabaseBM.limit_memory_usage(false, this._n_pages);
                return null;
            }
            this.outer_candidate.tupleCopy(outer_candidate_temp);
            if ( this._status == true )
            {
                try
                {
                	/* another scan on the data heap file */
                    this._inner_scan = this._heap_file.openScan();
                }
                catch (Exception e)
                {
                    this._status = false;
                    System.err.println ("*** Error opening scan\n");
                    e.printStackTrace();
                }
            }
            boolean inner_scan_complete = false;
            boolean inner_dominates_outer = false;
            while (!inner_scan_complete)
            {
                this.inner_candidate_temp = this._inner_scan.getNext(temp);
                if (this.inner_candidate_temp == null)
                {
                    inner_scan_complete = true;
                }
                else
                {
                    /* compare the outer loop tuple with inner loop tuple */
                	this.inner_candidate.tupleCopy(inner_candidate_temp);
                    inner_dominates_outer = TupleUtils.Dominates(this.inner_candidate,
                            									 this._in1,
                            									 this.outer_candidate,
                            									 this._in1,
                            									 this._len_in1,
                            									 this._t1_str_sizes,
                            									 this._pref_list,
                            									 this._pref_list_length);

                    if (inner_dominates_outer) {
                        break;
                    }
                }
            }
            if (inner_dominates_outer == false)
            {
            	/* no one dominated the outer loop tuple and hence it belongs to the skyline */
                return this.outer_candidate;
            }
            this._inner_scan.closescan();
        }
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
        }
    }

}

