package btree;

import java.io.IOException;

import global.Convert;
import global.PageId;
import global.RID;
import heap.FileAlreadyDeletedException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.SpaceNotAvailableException;
import heap.Tuple;

/**
 * A simple implementation of simple array list datastructure using {@link Heapfile}<br>
 * since minibase has fixed length records <br>
 * the insertion order is maintained in heapfiles<br>
 * <br>
 * currently only used in the BTreeSky code <br>
 * currently only supports {@link RID} elements
 */
public class DiskBackedArray {

	
	private Heapfile heapfile;

	public DiskBackedArray(String heapfilenameSuffix) throws HFException, HFBufMgrException, HFDiskMgrException, IOException  {
		heapfile = new Heapfile(new String("temp"+heapfilenameSuffix));
	}
	
	public Heapfile getHeapfile() {
		return heapfile;
	}
	
	/**
	 * Insert a RID into the heapfile
	 * @param elementToAdd
	 * @return RID in the temporary heapfile ( NOTE: this is name same as the elementToAdd)
	 */
	public RID add(RID elementToAdd) throws IOException, InvalidSlotNumberException, InvalidTupleSizeException, SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException {
		byte[] byteArr = new byte[8];
		elementToAdd.writeToByteArray(byteArr, 0);
		return heapfile.insertRecord(byteArr);
	}
	
	public void printToConsole() throws InvalidTupleSizeException, IOException   {
		Scan scan = heapfile.openScan();
		RID rid = new RID();
		Tuple tup;
		boolean done = false;
		System.out.print("DiskBackedArray [ ");
		while(!done) {
			tup= scan.getNext(rid );
			if(tup==null) {
				done=true;
				break;
			}
			System.out.print(""+getRIDFromByteArr(tup.returnTupleByteArray())+" ");
		}
		System.out.println("]");
		scan.closescan();
	}
	
	/**
	 * Search the heapfile for the element and return index, else return -1
	 * @param ridToSearch
	 * @return index at which element found in the array
	 * @throws InvalidTupleSizeException
	 * @throws IOException
	 */
	public int getIndex(RID ridToSearch) throws InvalidTupleSizeException, IOException {
		int foundIndex = -1;
		Scan scan = heapfile.openScan();
		RID rid = new RID();
		Tuple tup;
		boolean done = false;
		int i =0;
		while(!done) {
			tup= scan.getNext(rid );
			if(tup==null) {
				done=true;
				break;
			}
			RID scannedRID = getRIDFromByteArr(tup.returnTupleByteArray());
			//System.out.println(i+" scannedRid: "+scannedRID);
			if(scannedRID.equals(ridToSearch)) {
				done=true;
				foundIndex = i;
				break;
			}
			i++;
		}
		scan.closescan();
		return foundIndex;
		
	}
	
	/**
	 * Delete the underlying heapfile
	 */
	public void close() {
		try {
			this.heapfile.deleteFile();
		} catch (FileAlreadyDeletedException e) {
		} catch (Exception e) {
			System.err.println("Exception while deleting healpfile: " + e.getMessage());
		}
	}
	
	/**
	 * Deserialize bytearray into an RID
	 * @param bytearr
	 * @return
	 * @throws IOException
	 */
	public static RID getRIDFromByteArr(byte[] bytearr) throws IOException {
		int slotNo=Convert.getIntValue(0, bytearr);
		int pageNo=Convert.getIntValue(4, bytearr);
		return new RID(new PageId(pageNo), slotNo);
	}
	
}
