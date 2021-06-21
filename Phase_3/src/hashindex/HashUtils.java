package hashindex;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import btree.AddFileEntryException;
import btree.GetFileEntryException;
import global.AttrType;
import global.PageId;
import global.SystemDefs;

public class HashUtils {

	public final static int getKeyLength(HashKey key) throws IOException {
		switch (key.type) {
		case AttrType.attrInteger:
			return 4;
		case AttrType.attrReal:
			return 4;
		case AttrType.attrString:
			OutputStream out = new ByteArrayOutputStream();
			DataOutputStream outstr = new DataOutputStream(out);
			outstr.writeUTF(((String) key.value));
			return outstr.size();
		default:
			throw new IOException("key types do not match");
		}

	}

	public static byte[] intToBytes(int x) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bos);
		out.writeInt(x);
		out.close();
		byte[] int_bytes = bos.toByteArray();
		bos.close();
		return int_bytes;
	}

	//util function to debug log stuff
	// if no debug log wanted, comment the print statement
	public static void log(String str) {
		if(false)System.out.println(str); //comment this line to disable logs
		return;
	}

	public static void printPinnedPages() {
		System.out.println(Thread.currentThread().getStackTrace()[2].getMethodName()+"pin: "+(SystemDefs.JavabaseBM.getNumBuffers()- SystemDefs.JavabaseBM.getNumUnpinnedBuffers()));

	}
	
	// static methods ///////
	public static PageId get_file_entry(String filename) throws GetFileEntryException {
		try {
			return SystemDefs.JavabaseDB.get_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e, "");
		}
	}

	public static void add_file_entry(String fileName, PageId pageno) throws AddFileEntryException {
		try {
			SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new AddFileEntryException(e, "");
		}
	}

	/**
	 * A wrapper to store 2 values<br>
	 * useful if we want to return 2 values from a function
	 * <pre>{@code
	 * Pair<String,Integer> pair = new Pair<>();
	 * pair.first="hello";
	 * pair.second=1;
	 * return pair;
	 * }</pre>
	 * @param <M>
	 * @param <N>
	 */
	public static class Pair<M,N> {
		public M first;
		public N second;
	}
}
