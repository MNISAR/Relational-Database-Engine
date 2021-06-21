/*
 * @(#) bt.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *        Author Xiaohu Li (xiaohu@cs.wisc.edu)
 */
package btree;
import global.*;

/** KeyDataEntry: define (key, data) pair.
 */
public class Key {
   /** key in the (key, data)
    */  
   public KeyClass key;
   
  /** Class constructor
   */
  public Key( Integer key) {
     this.key = new IntegerKey(key); 
  }; 
  
  public Key( Float key) {
	     this.key = new FloatKey(key);
  }; 

  /** Class constructor.
   */
  public Key( KeyClass key) {
     if ( key instanceof IntegerKey ) 
        this.key= new IntegerKey(((IntegerKey)key).getKey());
     else if ( key instanceof StringKey ) 
        this.key= new StringKey(((StringKey)key).getKey());
     else if(key instanceof FloatKey)
    	 this.key = new FloatKey(((FloatKey)key).getKey());
  };


  /** Class constructor.
   */
  public Key( String key) {
     this.key = new StringKey(key); 
  };

  /** shallow equal. 
   *  @param entry the entry to check again key. 
   *  @return true, if entry == key; else, false.
   */
  public boolean equals(Key entry) {
      boolean st1,st2;

      if ( key instanceof IntegerKey )
         st1= ((IntegerKey)key).getKey().equals
                  (((IntegerKey)entry.key).getKey());
      else if ( key instanceof FloatKey )
          st1= ((FloatKey)key).getKey().equals
                   (((FloatKey)entry.key).getKey());
      else 
         st1= ((StringKey)key).getKey().equals
                  (((StringKey)entry.key).getKey());


  
      return (st1);
  }     
}

