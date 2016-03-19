package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	/**
	 * Constructs a heap file backed by the specified file.
	 * 
	 * @param f
	 *            the file that stores the on-disk backing store for this heap
	 *            file.
	 */

	private File heapFile;
	private TupleDesc tDesc;

	public HeapFile(File f, TupleDesc td) {
		// some code goes here
		heapFile = f;
		tDesc = td;
	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 * 
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		// some code goes here
		return heapFile;
	}

	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note:
	 * you will need to generate this tableid somewhere ensure that each
	 * HeapFile has a "unique id," and that you always return the same value for
	 * a particular HeapFile. We suggest hashing the absolute file name of the
	 * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 * 
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {
		// some code goes here
		// throw new UnsupportedOperationException("implement this");
		return heapFile.getAbsolutePath().hashCode();
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		// throw new UnsupportedOperationException("implement this");

		return tDesc;
	}

	// see DbFile.java for javadocs
	public Page readPage(PageId pid) {
		// some code goes here
		int pageSize = Database.getBufferPool().getPageSize();
		byte[] bytes = new byte[pageSize];
		HeapPageId id = new HeapPageId(pid.getTableId(), pid.pageNumber());
		HeapPage page = null;
		try {
			RandomAccessFile reader = new RandomAccessFile(heapFile, "r");

			long offset = 1L * pid.pageNumber() * pageSize;
			reader.seek(offset);
			reader.read(bytes);
			page = new HeapPage(id, bytes);
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return page;
	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		// some code goes here
		try {
			RandomAccessFile writer = new RandomAccessFile(heapFile, "rw");
			PageId pid = page.getId();
			int pageSize = Database.getBufferPool().getPageSize();
			long offset = 1L * pid.pageNumber() * pageSize;
			writer.seek(offset);
			byte[] bytes = page.getPageData();
			assert bytes.length == pageSize;
			writer.write(bytes, 0, bytes.length);
			writer.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		// some code goes here
		long fileSize = heapFile.length();
		int pageSize = Database.getBufferPool().getPageSize();
		return (int) (fileSize / pageSize);
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		int tableId = getId();
		int numPages = numPages();
		ArrayList<Page> affected = new ArrayList<Page>();

		for (int curPage = 0; curPage < numPages; curPage++) {
			HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, curPage),
					Permissions.READ_WRITE);
			if (page.getNumEmptySlots() > 0) {
				page.insertTuple(t);
				affected.add(page);
				return affected;
			}
		}

		// create a new Page
		HeapPage page = new HeapPage(new HeapPageId(tableId, numPages), HeapPage.createEmptyPageData());
		page.insertTuple(t);
		writePage(page);
		affected.add(page);
		return affected;
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
		// some code goes here
		BufferPool pool = Database.getBufferPool();
		HeapPage page = (HeapPage) pool.getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
		page.deleteTuple(t);
		ArrayList<Page> affected = new ArrayList<Page>();
		affected.add(page);
		return affected;
	}

	public static class HeapFileInterator implements DbFileIterator {

		private HeapFile heapFile;
		private TransactionId tid;
		private int curPage;
		private final int tableId;
		private final int numPages;
		private Iterator<Tuple> tupleIter;
		private Tuple next = null;

		HeapFileInterator(HeapFile heapFile, TransactionId tid) {
			this.heapFile = heapFile;
			this.tid = tid;
			this.tableId = this.heapFile.getId();
			this.numPages = this.heapFile.numPages();
			this.tupleIter = null;
			this.curPage = numPages + 1;
		}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			curPage = 0;
			BufferPool pool = Database.getBufferPool();
			HeapPage page = (HeapPage) pool.getPage(tid, new HeapPageId(this.tableId, curPage), Permissions.READ_WRITE);
			tupleIter = page.iterator();
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			if (tupleIter == null) {
				return false; // not open
			}
			if (next == null)
				next = fetchNext();
			return next != null;
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			// TODO Auto-generated method stub
			if (tupleIter == null) {
				throw new NoSuchElementException();
			}
			if (next == null) {
				next = fetchNext();
				if (next == null)
					throw new NoSuchElementException();
			}

			Tuple result = next;
			next = null;
			return result;
		}

		private Tuple fetchNext() throws TransactionAbortedException, DbException {
			// TODO Auto-generated method stub

			if (tupleIter.hasNext()) {
				return tupleIter.next();
			}
			while (curPage + 1 < numPages) {
				curPage++;
				HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.tableId, curPage),
						Permissions.READ_WRITE);
				tupleIter = page.iterator();
				if (tupleIter.hasNext()) {
					return tupleIter.next();
				}
			}
			return null;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			close();
			open();
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			tupleIter = null;
		}

	}

	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		// some code goes here
		return new HeapFileInterator(this, tid);
	}

}
