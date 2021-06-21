package driver;


import java.io.*;
import java.util.*;

import btree.*;
import bufmgr.*;
import chainexception.ChainException;
import diskmgr.PCounter;
import heap.*;
import global.*;

import index.IndexException;
import iterator.*;
import iterator.Iterator;
import tests.TestDriver;


//watching point: RID rid, some of them may not have to be newed.

public class testgenerator implements  GlobalConst{

	
	public testgenerator() {
		
	}
	
	public void test1() throws IOException {
		BufferedWriter out = null;
		Random rand = new Random();
		FileWriter fstream = new FileWriter("test1.txt", true);
		out = new BufferedWriter(fstream);
		out.write("5\n");
		for ( int i=0; i<5; i++ ) {
			out.write("Col"+i+"\tINT\n");
		}
		for ( int i=0; i<5000; i++ ) {
			int rand1 = rand.nextInt(10000);
			int rand2 = rand.nextInt(10000);
			int rand3 = rand.nextInt(10000);
			int rand4 = rand.nextInt(10000);
			int rand5 = rand.nextInt(10000);
			out.write(rand1+"\t"+rand2+"\t"+rand3+"\t"+rand4+"\t"+rand5+"\n");
		}
		int rand1 = rand.nextInt(10000);
		int rand2 = rand.nextInt(10000);
		int rand3 = rand.nextInt(10000);
		int rand4 = rand.nextInt(10000);
		int rand5 = rand.nextInt(10000);
		out.write(rand1+"\t"+rand2+"\t"+rand3+"\t"+rand4+"\t"+rand5);
		out.close();
		fstream.close();
	}
	
	public void test2() throws IOException {
		BufferedWriter out = null;
		Random rand = new Random();
		FileWriter fstream = new FileWriter("testtwo.txt", true);
		out = new BufferedWriter(fstream);
		out.write("6\n");
		for ( int i=0; i<5; i++ ) {
			out.write("Col"+i+"\tINT\n");
		}
		out.write("Col5"+"\tSTR\n");
		for ( int i=0; i<5000; i++ ) {
			int rand1 = rand.nextInt(10000);
			int rand2 = rand.nextInt(10000);
			int rand3 = rand.nextInt(10000);
			int rand4 = rand.nextInt(10000);
			int rand5 = rand.nextInt(10000);
			String s = getAlphaNumericString(rand.nextInt(20));
			out.write(rand1+"\t"+rand2+"\t"+rand3+"\t"+rand4+"\t"+rand5+"\t"+s+"\n");
		}
		int rand1 = rand.nextInt(10000);
		int rand2 = rand.nextInt(10000);
		int rand3 = rand.nextInt(10000);
		int rand4 = rand.nextInt(10000);
		int rand5 = rand.nextInt(10000);
		String s = getAlphaNumericString(1+rand.nextInt(20));
		out.write(rand1+"\t"+rand2+"\t"+rand3+"\t"+rand4+"\t"+rand5+"\t"+s);
		out.close();
		fstream.close();
	}
	
	public void test3() throws IOException {
		BufferedWriter out = null;
		Random rand = new Random();
		FileWriter fstream = new FileWriter("testthree.txt", true);
		out = new BufferedWriter(fstream);
		out.write("2\n");
		for ( int i=0; i<1; i++ ) {
			out.write("Name"+"\tSTR\n");
		}
		out.write("Age"+"\tINT\n");
		for ( int i=0; i<50; i++ ) {
			int rand1 = rand.nextInt(80);
			String s = getAlphaNumericString(rand.nextInt(20));
			out.write(s+"\t"+rand1+"\n");
		}
		int rand1 = rand.nextInt(80);
		String s = getAlphaNumericString(1+rand.nextInt(20));
		out.write(s+"\t"+rand1);
		out.close();
		fstream.close();
	}
	
	// function to generate a random string of length n
    private String getAlphaNumericString(int n)
    {
  
        // chose a Character random from this String
        String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                    + "0123456789"
                                    + "abcdefghijklmnopqrstuvxyz";
  
        // create StringBuffer size of AlphaNumericString
        StringBuilder sb = new StringBuilder(n);
  
        for (int i = 0; i < n; i++) {
  
            // generate a random number between
            // 0 to AlphaNumericString variable length
            int index
                = (int)(AlphaNumericString.length()
                        * Math.random());
  
            // add Character one by one in end of sb
            sb.append(AlphaNumericString
                          .charAt(index));
        }
        String s = sb.toString();
        if ( sb.length() == 0 ) {
        	return "Khushalmodiblah";
        }
        return s;
    }
	
    public static void main(String [] argvs) {

        try{
        	testgenerator driver = new testgenerator();
        	driver.test3();
            //driver.runTests();
        }
        catch (Exception e) {
            System.err.println ("Error encountered during running main driver:\n");
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }finally {

        }
    }

}