package simpledb;



/** A class to represent statistics for a single integer-based field.
 */
public class IntStatistics {

    // You made add any other fields you think are necessary.

    private int numTuples;
    private int numDistinctTuples;
    private final boolean[] distinctInts;
    private int high;
    private int low;
    private final static  int defaultSelectivity = 1/10;
    private boolean isFirstValue = true;
    // TODO: IMPLEMENT ME

    /**
     * Create a new IntStatistic.
     * 
     * This IntStatistic should maintain a statistics about the integer values that it receives.
     * 
     * The integer values will be provided one-at-a-time through the "addValue()" function.
     */
    public IntStatistics(int bins) {
        numTuples = 0;
        numDistinctTuples = 0;
        distinctInts = new boolean[bins];

        // TODO: IMPLEMENT ME
    }

    /**
     * Add a value to the set of values that you are tracking statistics for
     * @param v Value to add to the statistics
     */
    public void addValue(int v) {
        // TODO: IMPLEMENT ME
        //compare the value with high and low, and update them if value > high or value < low
        if (isFirstValue) {
            high = v;
            low = v;
            isFirstValue = false;
        } else {
            if (v > high) {
                high = v;
            } else if (v < low){
                low = v;
            }

        }

        // hashes the value and keeps an estimate to the number of distinct tuples we've seen
        int index = (hashCode(v) % distinctInts.length + distinctInts.length) % distinctInts.length;
        if (distinctInts[index] == false) {
            distinctInts[index] = true;
            numDistinctTuples++;
        }

        numTuples++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // the approximate number of distinct tuples we've seen in total
        double numDistinct = ((double) numTuples) * numDistinctTuples / distinctInts.length;

        // TODO: IMPLEMENT ME
        switch (op) {
            case EQUALS:
                if (v > high || v < low) {
                    return 0;
                }
                if (high ==low) {
                    if (v == high) {
                        return 1;
                    } else {
                        return 0;

                    }
                } else {

                        return ((double) 1) / numDistinct;


                }
            case GREATER_THAN:
            case GREATER_THAN_OR_EQ:
                if (v <= low) {
                    return 1;
                } else if (v > high) {
                    return 0;
                } else {
                    return ((double) (high - v)) / (high - low);
                }
            case LESS_THAN:
            case LESS_THAN_OR_EQ:
                if (v < low) {
                    return 0;
                } else if (v >= high) {
                    return 1;
                }
                if (high == low) {
                    if (v <= high) {
                        return 1;
                    } else {
                        return 0;
                    }
                }  else {
                    return ((double) (v - low)) / (high - low);
                }
            case NOT_EQUALS:
                if (v > high || v < low) {
                    return 1;
                }
                if (high == low) {
                    if (high != v) {
                        return 1;
                    } else {

                        return 0;
                    }
                } else {

                    return 1 - ( (double)1 / numDistinct);
                }


        }
        return defaultSelectivity;

    }

    /**
     * Helper function to make a good hash value of an integer
     */
    static int hashCode(int v) {
        v ^= (v >>> 20) ^ (v >>> 12);
        return v ^ (v >>> 7) ^ (v >>> 4);
    }
}
