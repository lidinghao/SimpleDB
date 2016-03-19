package simpledb;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;

	private int gbfield;
	private int afield;
	private Type gbfieldtype;
	private Op op;

	private HashMap<Field, Integer> numTuples, aggregatedValue;
	private TupleDesc tDesc;

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
		this.op = what;
		this.tDesc = null;

		this.aggregatedValue = new HashMap<Field, Integer>();
		this.numTuples = new HashMap<Field, Integer>();
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

		if (tDesc == null) {
			if (gbfieldtype == null) {
				tDesc = new TupleDesc(new Type[] { Type.INT_TYPE });
			} else {
				tDesc = new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE });
			}
		}

		int value = ((IntField) tup.getField(afield)).getValue();

		Field field = null;

		if (gbfieldtype != null) {
			field = tup.getField(gbfield);
		}

		// counting

		if (numTuples.containsKey(field)) {
			numTuples.put(field, numTuples.get(field) + 1);
		} else {
			numTuples.put(field, 1);
		}

		switch (op) {
		case AVG:
		case SUM:
			if (aggregatedValue.containsKey(field)) {
				aggregatedValue.put(field, aggregatedValue.get(field) + value);
			} else {
				aggregatedValue.put(field, value);
			}
			break;
		case COUNT:
			if (aggregatedValue.containsKey(field)) {
				aggregatedValue.put(field, aggregatedValue.get(field) + 1);
			} else {
				aggregatedValue.put(field, 1);
			}
			break;
		case MAX:
			if (aggregatedValue.containsKey(field)) {
				aggregatedValue.put(field, Math.max(aggregatedValue.get(field), value));
			} else {
				aggregatedValue.put(field, value);
			}
			break;
		case MIN:
			if (aggregatedValue.containsKey(field)) {
				aggregatedValue.put(field, Math.min(aggregatedValue.get(field), value));
			} else {
				aggregatedValue.put(field, value);
			}
			break;
		default:
			break;
		}

	}

	public static class IntegerAggregatorInterator extends Operator {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private Iterator<Field> numIter;
		private IntegerAggregator integerAggregator;

		IntegerAggregatorInterator(IntegerAggregator integerAggregator) {
			this.integerAggregator = integerAggregator;
			this.numIter = integerAggregator.numTuples.keySet().iterator();
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			this.numIter = integerAggregator.numTuples.keySet().iterator();
		}

		@Override
		protected Tuple fetchNext() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			if (numIter.hasNext()) {
				Field field = numIter.next();
				int value = integerAggregator.aggregatedValue.get(field);

				int result = 0;
				switch (integerAggregator.op) {
				case AVG:
					result = value / integerAggregator.numTuples.get(field);
					break;
				case COUNT:
					result = integerAggregator.numTuples.get(field);
					break;
				case MAX:
				case MIN:
				case SUM:
					result = value;
					break;
				default:
					break;
				}

				Tuple t = new Tuple(integerAggregator.tDesc);
				if (integerAggregator.gbfieldtype == null) {
					t.setField(0, new IntField(result));
				} else {
					t.setField(0, field);
					t.setField(1, new IntField(result));
				}
				return t;
			}
			return null;
		}

		@Override
		public DbIterator[] getChildren() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void setChildren(DbIterator[] children) {
			// TODO Auto-generated method stub
		}

		@Override
		public TupleDesc getTupleDesc() {
			// TODO Auto-generated method stub
			return integerAggregator.tDesc;
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
		return new IntegerAggregatorInterator(this);
	}

}
