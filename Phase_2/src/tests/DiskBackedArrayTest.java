package tests;

import btree.DiskBackedArray;
import global.PageId;
import global.RID;
import global.SystemDefs;

public class DiskBackedArrayTest {
	
	public static void main(String[] args) {

		try {
			String dbpath = "bla.db";
			SystemDefs sysdef = new SystemDefs(dbpath, 300, 300, "Clock");
			DiskBackedArray thiss = new DiskBackedArray("temp");

			for (int i = 0; i < 100; i++) {
				RID a = new RID(new PageId(i), i + 1);
				thiss.add(a);
			}
			System.out.println("" + thiss.getHeapfile().getRecCnt());
			RID searRID = new RID(new PageId(24), 25);
			int foundIndex = thiss.getIndex(searRID);
			System.out.println("foundindex: " + foundIndex);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
