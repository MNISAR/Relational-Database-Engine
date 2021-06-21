package global;

/**
 * Enumeration class for Aggregation type
 *
 */

public class AggType {

    public static final int MIN = 0;
    public static final int MAX = 1;
    public static final int AVG = 2;
    public static final int SKYLINE = 3;

    public int aggType;

    /**
     * TupleOrder Constructor
     * <br>
     * A tuple ordering can be defined as
     * <ul>
     * <li>   TupleOrder tupleOrder = new TupleOrder(TupleOrder.Random);
     * </ul>
     * and subsequently used as
     * <ul>
     * <li>   if (tupleOrder.tupleOrder == TupleOrder.Random) ....
     * </ul>
     *
     * @param _aggType The possible ordering of the tuples
     */

    public AggType (int _aggType) {
        aggType = _aggType;
    }

    public String toString() {

        switch (aggType) {
            case MIN:
                return "Minimum";
            case MAX:
                return "Maximum";
            case AVG:
                return "Average";
            case SKYLINE:
                return "Skyline";
        }
        return ("Unexpected TupleOrder " + aggType);
    }

}
