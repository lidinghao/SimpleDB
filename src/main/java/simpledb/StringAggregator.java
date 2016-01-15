package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    Type gbfieldtype;
    int aggregateField;
    boolean hasGroupBy;
    Op aggreator;
    private Map<Field,Integer> aggregateVal;
    private int noGroupByVal;
    private Map<Field,Field> gbMap;
private List<Tuple> aggregateResult;
    private boolean isFirst;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.aggregateField = afield;
        this.aggreator = what;
        this.gbMap = new HashMap<Field, Field>();
        this.aggregateResult = new LinkedList<>();
        this.aggregateVal = new HashMap<>();
        hasGroupBy = (gbfieldtype == null ? false : true);
        if (what != Op.COUNT)
            throw  new IllegalArgumentException("");
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (gbfieldtype == null){
            noGroupByVal++;

        }else{
            Field gbKey = tup.getField(gbfield);
            if(aggregateVal.containsKey(gbKey)) {
            int countSoFar  = aggregateVal.get(gbKey);
                switch (aggreator) {
                    case COUNT:
                        aggregateVal.put(gbKey,++countSoFar);

                }
            }else{
                aggregateVal.put(gbKey,1);
            }
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        return new DbIterator() {
            boolean isFirst = true;
            Iterator<Tuple> iter ;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                if (hasGroupBy){
                    for (Map.Entry<Field, Integer> entry : aggregateVal.entrySet()){
                        Tuple tuple = new Tuple(getTupleDesc());
                        tuple.setField(0, entry.getKey());
                        tuple.setField(1, new IntField(entry.getValue()));
                        aggregateResult.add(tuple);
                    }
                }else {
                    Tuple tuple = new Tuple(getTupleDesc());
                    tuple.setField(0, new IntField(noGroupByVal));
                    aggregateResult.add(tuple);
                }
                iter = aggregateResult.iterator();

            }
            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                    return iter.hasNext();
            }


            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                    return iter.next();


            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                iter = aggregateResult.iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                if (hasGroupBy){
                    return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});

                }else {
                    return new TupleDesc(new Type[]{Type.INT_TYPE});
                }
            }

            @Override
            public void close() {
iter = null;
            }
        };
        // some code goes here
    }

}
