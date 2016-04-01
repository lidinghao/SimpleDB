package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;

	private int gbfield;
	private int afield;
	private Type gbfieldtype;
	private Op op;

	private HashMap<Field, Integer> aggregatedCount;
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
	 *            aggregation operator to use -- only supports COUNT
	 * @throws IllegalArgumentException
	 *             if what != COUNT
	 */

	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.op = what;
		this.tDesc = null;
		this.aggregatedCount = new HashMap<Field, Integer>();

		if (what != Op.COUNT) {
			throw new IllegalArgumentException();
		}
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

		Field field = null;
		if (gbfieldtype != null) {
			field = tup.getField(gbfield);
		}

		// must be COUNT
		int value = 0;
		if (aggregatedCount.containsKey(field)) {
			value = aggregatedCount.get(field);
		}
		aggregatedCount.put(field, value + 1);
	}

	public static class StringAggregatorInterator extends Operator {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private Iterator<Field> keyIter;
		private StringAggregator stringAggregator;
		
		StringAggregatorInterator(StringAggregator stringAggregator) {
			this.stringAggregator = stringAggregator;
			this.keyIter = stringAggregator.aggregatedCount.keySet().iterator();
		}

		
		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			keyIter = stringAggregator.aggregatedCount.keySet().iterator();
		}

		@Override
		protected Tuple fetchNext() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			if (keyIter.hasNext()) {
				Field field = keyIter.next();
				int value = stringAggregator.aggregatedCount.get(field);
				Tuple t = new Tuple(stringAggregator.tDesc);
				if (stringAggregator.gbfieldtype == null) {
					t.setField(0, new IntField(stringAggregator.aggregatedCount.get(null)));
				} else {
					t.setField(0, field);
					t.setField(1, new IntField(value));
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
			return stringAggregator.tDesc;
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
		return new StringAggregatorInterator(this);
	}

}
