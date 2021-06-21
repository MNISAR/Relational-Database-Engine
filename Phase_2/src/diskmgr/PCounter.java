package diskmgr;

public class PCounter {
    public static int rcounter;
    public static int wcounter;

    public static void initialize() {
        rcounter =0;
        wcounter =0;
    }

    public static void readIncrement() {
        rcounter++;
    }

    public static void writeIncrement() {
        wcounter++;
    }
    
    public static int get_wcounter() {
    	return wcounter;
    }
    
    public static int get_rcounter() {
    	return rcounter;
    }
}
