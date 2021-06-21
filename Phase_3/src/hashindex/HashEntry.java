package hashindex;

import java.io.IOException;

import btree.KeyNotMatchException;
import global.Convert;
import global.PageId;
import global.RID;

public class HashEntry {

	public HashKey key;
	public RID rid;

	public HashEntry(HashKey searchkey, RID ridd) {
		key = new HashKey(searchkey);
		rid = new RID(ridd.pageNo, ridd.slotNo);
	}

	public HashEntry(byte data[], int offset) throws IOException, KeyNotMatchException {
		key = new HashKey(data, offset);
		rid = getRIDFromByteArr(data, offset + key.size());
	}

	public void writeToByteArray(byte arr[], int offset) throws IOException, KeyNotMatchException {
		key.writeToByteArray(arr, offset);
		rid.writeToByteArray(arr, offset + key.size());
	}

	public int size() {
		return 8 + key.size();// rid length is 8 bytes
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof HashEntry) {
			HashEntry ent = (HashEntry) obj;
			return key.equals(ent.key) && rid.equals(ent.rid);
		}
		return false;
	}

	@Override
	public String toString() {
		return "HashEntry [key=" + key + ", rid=" + rid + "]";
	}

	public static RID getRIDFromByteArr(byte[] bytearr, int offset) throws IOException {
		int slotNo = Convert.getIntValue(offset, bytearr);
		int pageNo = Convert.getIntValue(offset + 4, bytearr);
		return new RID(new PageId(pageNo), slotNo);
	}

}
