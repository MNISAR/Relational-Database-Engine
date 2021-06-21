package btree;

import java.io.*;
import java.security.Key;
import java.util.*;


import java.lang.*;

import heap.*;
import bufmgr.*;
import global.*;
import btree.*;

/**
 * Note that in JAVA, methods can't be overridden to be more private.
 * Therefore, the declaration of all private functions are now declared
 * protected as opposed to the private type in C++.
 */

public class GenerateIndexFiles{
    BTreeFile file;
    int id=0;
    public static int prefix = 0;
    public GenerateIndexFiles(){
    }

    /*
     * Method for padding string on right.
     */
    public static String padRight(String s, int n) {
        return String.format("%-" + n + "s", s).replace(' ', '0');
    }

    /*
     * This method create key by
     *  converting the double representation to its string version but with '0' padded to right
     *  AND "|" as seperator.
     */
    String create_key(double[] values) {
        String s;
        StringBuilder sb = new StringBuilder();
        for (double value : values) {
            s = padRight(String.valueOf(value), 10);
            sb.append(s);
            sb.append("|");
        }
        return sb.toString();
    }
    
    float create_key_float(Tuple t, AttrType[] attrType, int[] pref_list, int pref_list_length) {
		float sum = 0.0f;
		for(int i = 0; i < pref_list_length; i++) {
			if(pref_list[i] == 1) {
				if(attrType[i].attrType == AttrType.attrInteger){
                    try{
                        sum += t.getIntFld(i+1);
                    }
                    catch (Exception e){
                        System.out.println("cannot get int field: "+(i+1));
                        e.printStackTrace();
                    }
                }
				else if (attrType[i].attrType == AttrType.attrReal){
                    try{
                        sum += t.getFloFld(i+1);
                    }
                    catch (Exception e){
                        System.out.println("cannot get float field: "+(i+1));
                        e.printStackTrace();
                    }
                }
				else{
				    System.out.println("Only integer&real attribute types are supposted for Skyline computation.");
                }

			}
		}
		return sum;
	}

    private double[][] readFile(String filePath) throws FileNotFoundException {
        File dfile = new File(filePath);
        Scanner sc = new Scanner(dfile);
        int COLS = sc.nextInt();
        List<double[]> records = new ArrayList<double[]>();

        while (sc.hasNextLine()) {
            double[] doubleArray = Arrays.stream(Arrays.stream(sc.nextLine().trim()
                    .split("\\s+"))
                    .filter(s -> !s.isEmpty())
                    .toArray(String[]::new))
                    .mapToDouble(Double::parseDouble)
                    .toArray();
            if(doubleArray.length != 0){
                records.add(doubleArray);
            }
        }
        double [][] ret = new double[records.size()][];
        ret = records.toArray(ret);
        return ret;
    }

    public IndexFile createCombinedBTreeIndex(String relationName, AttrType[] attrType, int[] pref_list, int pref_list_length, short[] aSizes)
            throws IOException, AddFileEntryException, GetFileEntryException, ConstructPageException, HashEntryNotFoundException, IteratorException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException, NodeNotMatchException, UnpinPageException, LeafInsertRecException, IndexSearchException, InsertException, PinPageException, ConvertException, DeleteRecException, KeyNotMatchException, LeafDeleteException, KeyTooLongException, IndexInsertRecException, HFDiskMgrException, HFBufMgrException, HFException, FieldNumberOutOfBoundException, InvalidSlotNumberException, SpaceNotAvailableException, InvalidTupleSizeException, InvalidTypeException {

        String filename = relationName;
        String btree_index_name = filename + ".btreeindex" + ++prefix;

        int keyType = AttrType.attrReal;
        int keySize = 4;

        Heapfile heapfile = new Heapfile(filename);
        Scan heap_scan = heapfile.openScan();

        file = new BTreeFile(btree_index_name, keyType, keySize, 0);

//        AttrType [] Stypes = new AttrType[pref_list_length];
//        for(int i=0;i<pref_list_length;i++){Stypes[i] = new AttrType (AttrType.attrReal);}
        Tuple t = new Tuple();
//        short [] Ssizes = null;

        t.setHdr((short) pref_list_length, attrType, aSizes);
        int size = t.size();
        t = new Tuple(size);


        RID rid = new RID();
        
        float fkey;
        KeyClass ffkey;
        
        //TBD modify this portion of the code to handle input file as a heap file and also create proper keys 
        while((t=heap_scan.getNext(rid)) != null)
        {
            t.setHdr((short) pref_list_length, attrType, aSizes);
            fkey = create_key_float(t, attrType, pref_list, pref_list_length);
            ffkey = new FloatKey(-fkey);

            file.insert(ffkey, rid);
        }

        return file;
    }

    public void close() throws DeleteFileEntryException, IteratorException, PinPageException, IOException, ConstructPageException, FreePageException, UnpinPageException {
        file.destroyFile();
    }

    public IndexFile[] createBTreeIndex (String filePath) throws IOException, AddFileEntryException, GetFileEntryException, ConstructPageException, HashEntryNotFoundException, IteratorException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException, NodeNotMatchException, UnpinPageException, LeafInsertRecException, IndexSearchException, InsertException, PinPageException, ConvertException, DeleteRecException, KeyNotMatchException, LeafDeleteException, KeyTooLongException, IndexInsertRecException, HFDiskMgrException, HFBufMgrException, HFException, InvalidTupleSizeException, InvalidTypeException, FieldNumberOutOfBoundException, SpaceNotAvailableException, InvalidSlotNumberException {
        double[][] records = readFile(filePath);
        int ROWS = records.length;
        int COLS = records[ROWS-1].length;

        int keyType = AttrType.attrString;
        int keySize = 1 + (13 * 1);
        String filename = "AAA"+prefix++;

        IndexFile[] indices = new IndexFile[COLS];
        Heapfile heapfile = new Heapfile(filename);
        AttrType [] Stypes = new AttrType[COLS];
        for(int i=0;i<COLS;i++){Stypes[i] = new AttrType (AttrType.attrReal);}
        Tuple t = new Tuple();
        short [] Ssizes = null;
        t.setHdr((short) COLS,Stypes, Ssizes);
        int size = t.size();
        t = new Tuple(size);
        t.setHdr((short) COLS, Stypes, Ssizes);

        for(int j=0;j<COLS;j++) {
            filename = "AAA" + prefix++;
            indices[j] = new BTreeFile(filename, keyType, keySize, 1);
        }

        KeyClass key;
        RID rid = new RID();
        PageId pageno = new PageId();
        String skey;
        double[] value = null;
        for(int i=0;i<ROWS;i++) {
            value = records[i];
            for (int k = 0; k < value.length; k++) {
                t.setFloFld(k + 1, (float) value[k]);
            }
            rid = heapfile.insertRecord(t.returnTupleByteArray());
            for (int j = 0; j < COLS; j++) {
                skey = create_key(new double[]{value[j]});
                key = new StringKey(skey);
                indices[j].insert(key, rid);
            }
        }
        return indices;

    }
}

//class Main {
//    public static void main(String[] args) throws IOException, ConstructPageException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, GetFileEntryException, IteratorException, ReplacerException, AddFileEntryException, NodeNotMatchException, UnpinPageException, LeafInsertRecException, IndexSearchException, IndexInsertRecException, PinPageException, KeyTooLongException, DeleteRecException, KeyNotMatchException, LeafDeleteException, InsertException, ConvertException {
//        String filePath = "driver/data/subset.txt";
//        GenerateIndexFiles generatorObject = new GenerateIndexFiles();
//        BTreeFile indexFile = generatorObject.readFile(filePath);
//    }
//}