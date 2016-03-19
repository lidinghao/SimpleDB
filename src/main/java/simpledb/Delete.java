package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

	private static final long serialVersionUID = 1L;

	private TransactionId tid;
	private DbIterator child;
	private boolean deleted;
	private TupleDesc tDesc;

	/**
	 * Constructor specifying the transaction that this delete belongs to as
	 * well as the child to read from.
	 * 
	 * @param t
	 *            The transaction this delete runs in
	 * @param child
	 *            The child operator from which to read tuples for deletion
	 */
	public Delete(TransactionId t, DbIterator child) {
		// some code goes here
		this.tid = t;
		this.child = child;
		this.deleted = false;
		this.tDesc = new TupleDesc(new Type[] { Type.INT_TYPE });
	}

	public TupleDesc getTupleDesc() {
		// some code goes here
		return tDesc;
	}

	public void open() throws DbException, TransactionAbortedException {
		// some code goes here
		child.open();
		super.open();
	}

	public void close() {
		// some code goes here
		super.close();
		child.close();
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		child.rewind();
	}

	/**
	 * Deletes tuples as they are read from the child operator. Deletes are
	 * processed via the buffer pool (which can be accessed via the
	 * Database.getBufferPool() method.
	 * 
	 * @return A 1-field tuple containing the number of deleted records.
	 * @see Database#getBufferPool
	 * @see BufferPool#deleteTuple
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if (this.deleted) {
			return null;
		}
		this.deleted = true;

		BufferPool pool = Database.getBufferPool();
		int numDeleted = 0;
		while (child.hasNext()) {
			Tuple tDelete = child.next();
			try {
				pool.deleteTuple(tid, tDelete);
				numDeleted++;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		Tuple t = new Tuple(tDesc);
		t.setField(0, new IntField(numDeleted));
		return t;
	}

	@Override
	public DbIterator[] getChildren() {
		// some code goes here
		return new DbIterator[] { child };
	}

	@Override
	public void setChildren(DbIterator[] children) {
		// some code goes here
		child = children[0];
	}

}
