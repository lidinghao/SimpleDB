package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructor. Accepts to children to join and the predicate to join them
	 * on
	 * 
	 * @param p
	 *            The predicate to use to join the children
	 * @param child1
	 *            Iterator for the left(outer) relation to join
	 * @param child2
	 *            Iterator for the right(inner) relation to join
	 */

	private JoinPredicate joinPredicate;
	private DbIterator child1, child2;
	private TupleDesc tDesc;

	private Tuple tuple1, tuple2;

	public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
		// some code goes here
		this.joinPredicate = p;
		this.child1 = child1;
		this.child2 = child2;
		TupleDesc td1 = child1.getTupleDesc();
		TupleDesc td2 = child2.getTupleDesc();
		tDesc = TupleDesc.merge(td1, td2);

		tuple1 = null;
		tuple2 = null;
	}

	public JoinPredicate getJoinPredicate() {
		// some code goes here
		return joinPredicate;
	}

	/**
	 * @return the field name of join field1. Should be quantified by alias or
	 *         table name.
	 */
	public String getJoinField1Name() {
		// some code goes here
		int field = joinPredicate.getField1();
		return child1.getTupleDesc().getFieldName(field);
	}

	/**
	 * @return the field name of join field2. Should be quantified by alias or
	 *         table name.
	 */
	public String getJoinField2Name() {
		// some code goes here
		int field = joinPredicate.getField2();
		return child2.getTupleDesc().getFieldName(field);
	}

	/**
	 * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
	 *      implementation logic.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		return tDesc;
	}

	public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
		// some code goes here
		child1.open();
		child2.open();
		super.open();
	}

	public void close() {
		// some code goes here
		super.close();
		child1.close();
		child2.close();
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		child1.rewind();
		child2.rewind();

		tuple1 = null;
		tuple2 = null;
	}

	/**
	 * Returns the next tuple generated by the join, or null if there are no
	 * more tuples. Logically, this is the next tuple in r1 cross r2 that
	 * satisfies the join predicate. There are many possible implementations;
	 * the simplest is a nested loops join.
	 * <p>
	 * Note that the tuples returned from this particular implementation of Join
	 * are simply the concatenation of joining tuples from the left and right
	 * relation. Therefore, if an equality predicate is used there will be two
	 * copies of the join attribute in the results. (Removing such duplicate
	 * columns can be done with an additional projection operator if needed.)
	 * <p>
	 * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
	 * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
	 * 
	 * @return The next matching tuple.
	 * @see JoinPredicate#filter
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		while (child1.hasNext() || tuple1 != null) {
			if (tuple1 == null) {
				tuple1 = child1.next();
				child2.rewind();
			}
			while (child2.hasNext()) {
				tuple2 = child2.next();
				if (joinPredicate.filter(tuple1, tuple2)) {
					Tuple t = new Tuple(tDesc);
					int numFields1 = tuple1.getTupleDesc().numFields();
					int numFields2 = tuple2.getTupleDesc().numFields();
					for (int i = 0; i < numFields1; i++) {
						t.setField(i, tuple1.getField(i));
					}
					for (int i = 0; i < numFields2; i++) {
						t.setField(i + numFields1, tuple2.getField(i));
					}
					return t;
				}
			}
			tuple1 = null;
		}

		return null;

	}

	@Override
	public DbIterator[] getChildren() {
		// some code goes here
		return new DbIterator[] { child1, child2 };
	}

	@Override
	public void setChildren(DbIterator[] children) {
		// some code goes here
		child1 = children[0];
		child2 = children[1];
	}

}
