package iterator;

import java.util.Comparator;

import heap.Tuple;

public class NRABounds {

	public float lVal1 = 0.0f;
	public float lVal2 = 0.0f;
	
	public float uVal1 = 500.0f;
	public float uVal2 = 500.0f;
	
	public boolean isFullySeen = false;

	public String createBy = null; //REL1 or REL2
	
	Tuple t1 = null;
	Tuple t2 = null;

	public NRABounds(float val, String createBy) {
		this.lVal1 = val;
		this.uVal1 = val;
		this.createBy = createBy;
	}
	
	public void updateBounds(float newVal) {
		this.lVal2 = newVal;
		this.uVal2 = newVal;
	}
	
	public float getLowerBoundVal() {
		return lVal1 + lVal2;
	}
	
	public float getUpperBoundVal() {
		return uVal1 + uVal2;
	}
	
	public String toString() {
		String res = "";
		
		res += "LOW: (" + (lVal1 + lVal2) + ")" + "UPPER: (" + (uVal1  + uVal2)  + ")";
		return res;
	}
	
}

class NRABoundsComparator implements Comparator<NRABounds>{
 
    public int compare(NRABounds s1, NRABounds s2) {
        if (s1.getLowerBoundVal() < s2.getLowerBoundVal())
            return 1;
        else if (s1.getLowerBoundVal() > s2.getLowerBoundVal())
            return -1;
                       
        return 0;
     }
}