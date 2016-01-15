package simpledb;

import java.io.IOException;
import java.util.Vector;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator[] children;
    private Vector<Tuple> numsVec;

    private boolean hasFetched = false;
    private DbIterator child;
    private TransactionId tid;
    private int tableId;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableId = tableid;
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableid))) {
            throw new DbException("TupleDesc doesn't match");
        }
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();

    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        hasFetched =false;
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
         int num = 0;
        if (!hasFetched) {
            hasFetched = true;
            try {
                child.open();
                while (child.hasNext()){
                    try {
                        Database.getBufferPool().insertTuple(tid,tableId,child.next());
                        num ++;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            }
            Tuple tuple = new Tuple(getTupleDesc());
            tuple.setField(0, new IntField(num));
            return tuple;
        } else {
            return null;
        }


    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        children = children;
    }
}
