package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op aggregator;
    private HashMap<Field,Field> gbMap;
    private HashMap<Field,Integer> aggregateVal;
    private HashMap<Field,Integer> aggregateCount;
    private int noGroupByVal;
    private int noGroupByCount;
    private boolean isFirst;
    private boolean hasOpen;
    private int noGroupByResult;


    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.aggregator = what;
        this.gbMap = new HashMap<Field,Field>();
        this.aggregateVal  = new HashMap<Field,Integer>();
        this.aggregateCount = new HashMap<Field,Integer>();

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        IntField field = (IntField) tup.getField(afield);
        int value = field.getValue();
        if(gbfieldtype == null){
            switch (aggregator){
                case MIN:
                    noGroupByVal = Math.min(value, noGroupByVal);
                    break;
                case MAX:
                    noGroupByVal = Math.max(value, noGroupByVal);
                    break;
                case SUM:
                    noGroupByVal += value;
                    break;
                case COUNT:
                    noGroupByCount++;
                    break;
                case AVG:
                    noGroupByCount++;
                    noGroupByVal += value;

            }
        }else {
            IntField gbKey = (IntField) tup.getField(gbfield);
            if (aggregateVal.containsKey(gbKey)) {
                int valSoFar = aggregateVal.get(gbKey);
                switch (aggregator) {
                    case MIN:
                        aggregateVal.put(gbKey,Math.min(valSoFar,value));
                        break;
                    case MAX:
                        aggregateVal.put(gbKey,Math.max(valSoFar,value));
                        break;
                    case SUM:
                        aggregateVal.put(gbKey,valSoFar + value);
                        break;
                    case COUNT:
                        aggregateVal.put(gbKey, ++valSoFar);
                        break;
                    case AVG:
                        aggregateVal.put(gbKey, valSoFar + value);
                        int countSoFar = aggregateCount.get(gbKey);
                        aggregateCount.put(gbKey, ++countSoFar);

                }
            } else {
                switch (aggregator)  {
                    case COUNT:
                      aggregateVal.put(gbKey, 1);
                        break;
                    case AVG:
                        aggregateVal.put(gbKey,value);
                        aggregateCount.put(gbKey, 1);
                        break;
                    default:
                        aggregateVal.put(gbKey,value);

                }
            }
        }

    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new DbIterator() {
            Iterator<Map.Entry<Field,Field>> iter= null;
            boolean hasOpen = false;
            @Override
            public void open() throws DbException, TransactionAbortedException {
               if (hasOpen)
                   return;
                else
                   hasOpen = true;
               if(gbfieldtype == null){
                  switch (aggregator){
                      case AVG:
                          noGroupByResult = noGroupByVal / noGroupByCount;
                          break;
                      case COUNT:
                          noGroupByResult = noGroupByCount;
                          break;
                      default:
                          noGroupByResult = noGroupByVal;

                  }
               }else {
                   switch (aggregator) {
                       case AVG:
                           for (Map.Entry<Field, Integer> entry : aggregateVal.entrySet()) {
                               Field groupVal = entry.getKey();
                               int sum = entry.getValue();
                               int count = aggregateCount.get(groupVal);
                               IntField avg = new IntField(sum / count);
                               gbMap.put(groupVal, avg);
                           }
                           break;
                       case COUNT:
                           for (Map.Entry<Field, Integer> entry : aggregateVal.entrySet()) {
                               Field groupVal = entry.getKey();
                               IntField value = new IntField(entry.getValue());
                               gbMap.put(groupVal, value);
                           }
                           break;

                       default:
                           for (Map.Entry<Field, Integer> entry : aggregateVal.entrySet()) {
                               Field groupVal = entry.getKey();
                               IntField value = new IntField(entry.getValue());
                               gbMap.put(groupVal, value);
                           }


                   }
                   iter = gbMap.entrySet().iterator();
               }

            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (gbfieldtype == null)
                    return !isFirst;
                else
                    return iter.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                Tuple tuple = new Tuple(getTupleDesc());
                if (gbfieldtype == null){

                    tuple.setField(0,new IntField(noGroupByResult));
                    isFirst = true;

                }else {
                    Map.Entry<Field, Field> entry = iter.next();
                    tuple.setField(0, entry.getKey());
                    tuple.setField(1, entry.getValue());
                }
                return tuple;

            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if (gbfieldtype == null){
                    isFirst = false;
                }else
                   iter = gbMap.entrySet().iterator();

            }

            @Override
            public TupleDesc getTupleDesc() {
                if(gbfieldtype == null)
                    return new TupleDesc(new Type[]{Type.INT_TYPE});
                else
                return new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE});
            }

            @Override
            public void close() {
                iter = null;

            }
        };
    }

}
