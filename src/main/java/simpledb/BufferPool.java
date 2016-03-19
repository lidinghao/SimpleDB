package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
	/** Bytes per page, including header. */
	private static final int PAGE_SIZE = 4096;

	private static int pageSize = PAGE_SIZE;

	/**
	 * Default number of pages passed to the constructor. This is used by other
	 * classes. BufferPool should use the numPages argument to the constructor
	 * instead.
	 */
	public static final int DEFAULT_PAGES = 50;

	private HashMap<PageId, Page> bufferPool;
	private HashMap<TransactionId, HashSet<PageId>> abortedTxns;
	private final int pageLimit;

	private LockManager lockManager;

	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 *
	 * @param numPages
	 *            maximum number of pages in this buffer pool.
	 */

	public BufferPool(int numPages) {
		// some code goes here
		bufferPool = new HashMap<PageId, Page>();
		abortedTxns = new HashMap<TransactionId, HashSet<PageId>>();
		pageLimit = numPages;
		lockManager = new LockManager();
	}

	public static int getPageSize() {
		return pageSize;
	}

	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void setPageSize(int pageSize) {
		BufferPool.pageSize = pageSize;
	}

	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void resetPageSize() {
		BufferPool.pageSize = PAGE_SIZE;
	}

	/**
	 * Retrieve the specified page with the associated permissions. Will acquire
	 * a lock and may block if that lock is held by another transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool. If it is
	 * present, it should be returned. If it is not present, it should be added
	 * to the buffer pool and returned. If there is insufficient space in the
	 * buffer pool, an page should be evicted and the new page should be added
	 * in its place.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the page
	 * @param pid
	 *            the ID of the requested page
	 * @param perm
	 *            the requested permissions on the page
	 */
	public Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException {
		// some code goes here

		int retry = 0;
		Page page = null;

		if (perm.equals(Permissions.READ_ONLY)) {
			while (lockManager.rlock(pid, tid) == false) {
				retry++;
				if (retry > 5) {
					throw new TransactionAbortedException();
				}
				try {
					Thread.sleep(20);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} else {
			while (lockManager.wlock(pid, tid) == false) {
				retry++;
				if (retry > 5) {
					throw new TransactionAbortedException();
				}
				try {
					Thread.sleep(20);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			if (abortedTxns.containsKey(tid) == false) {
				abortedTxns.put(tid, new HashSet<PageId>());
			}
			abortedTxns.get(tid).add(pid);
		}
		synchronized (this) {
			if (bufferPool.containsKey(pid)) {
				page = bufferPool.get(pid);
			} else {
				page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
				if (bufferPool.size() == pageLimit) {
					evictPage();
				}
				bufferPool.put(pid, page);
			}
		}
		return page;
	}

	/**
	 * Releases the lock on a page. Calling this is very risky, and may result
	 * in wrong behavior. Think hard about who needs to call this and why, and
	 * why they can run the risk of calling it.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param pid
	 *            the ID of the page to unlock
	 */
	public void releasePage(TransactionId tid, PageId pid) {
		// some code goes here
		// not necessary for lab1|lab2

		lockManager.unlock(pid, tid);
	}

	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2

		transactionComplete(tid, true);
	}

	/**
	 * Return true if the specified transaction has a lock on the specified page
	 */
	public boolean holdsLock(TransactionId tid, PageId p) {
		// some code goes here
		// not necessary for lab1|lab2
		return lockManager.holdsLock(p, tid);
	}

	/**
	 * Commit or abort a given transaction; release all locks associated to the
	 * transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param commit
	 *            a flag indicating whether we should commit or abort
	 */
	public void transactionComplete(TransactionId tid, boolean commit) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
		synchronized (this) {

			HashSet<PageId> dirtyPages = abortedTxns.get(tid);
			if (dirtyPages != null) {
				for (PageId pid : dirtyPages) {
					if (commit) {
						flushPage(pid);
					} else {
						if (bufferPool.containsKey(pid)) {
							discardPage(pid);
						}
					}
				}
			}
			abortedTxns.remove(tid);
			lockManager.releaseLocks(tid);
		}

	}

	/**
	 * Add a tuple to the specified table on behalf of transaction tid. Will
	 * acquire a write lock on the page the tuple is added to and any other
	 * pages that are updated (Lock acquisition is not needed for lab2). May
	 * block if the lock(s) cannot be acquired.
	 * 
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markPageDirty bit, and adds versions of any pages that have been
	 * dirtied to the cache (replacing any existing versions of those pages) so
	 * that future requests see up-to-date pages.
	 *
	 * @param tid
	 *            the transaction adding the tuple
	 * @param tableId
	 *            the table to add the tuple to
	 * @param t
	 *            the tuple to add
	 */
	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
		DbFile DbFile = Database.getCatalog().getDatabaseFile(tableId);

		ArrayList<Page> pages = DbFile.insertTuple(tid, t);
		// mark them as dirty
		synchronized (this) {
			for (Page p : pages) {
				p.markPageDirty(true, tid);
				bufferPool.put(p.getId(), p);
			}
		}
	}

	/**
	 * Remove the specified tuple from the buffer pool. Will acquire a write
	 * lock on the page the tuple is removed from and any other pages that are
	 * updated. May block if the lock(s) cannot be acquired.
	 *
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markPageDirty bit, and adds versions of any pages that have been
	 * dirtied to the cache (replacing any existing versions of those pages) so
	 * that future requests see up-to-date pages.
	 *
	 * @param tid
	 *            the transaction deleting the tuple.
	 * @param t
	 *            the tuple to delete
	 */
	public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		DbFile DbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());

		ArrayList<Page> pages = DbFile.deleteTuple(tid, t);
		// mark them as dirty

		synchronized (this) {
			for (Page p : pages) {
				p.markPageDirty(true, tid);
				bufferPool.put(p.getId(), p);
			}
		}
	}

	/**
	 * Flush all dirty pages to disk. NB: Be careful using this routine -- it
	 * writes dirty data to disk so will break simpledb if running in NO STEAL
	 * mode.
	 */
	public synchronized void flushAllPages() throws IOException {
		// some code goes here

		for (PageId pid : bufferPool.keySet()) {
			flushPage(pid);
		}

	}

	/**
	 * Remove the specific page id from the buffer pool. Needed by the recovery
	 * manager to ensure that the buffer pool doesn't keep a rolled back page in
	 * its cache.
	 * 
	 * Also used by B+ tree files to ensure that deleted pages are removed from
	 * the cache so they can be reused safely
	 */
	public synchronized void discardPage(PageId pid) {
		// some code goes here
		// not necessary for lab1

		bufferPool.remove(pid);
	}

	/**
	 * Flushes a certain page to disk
	 * 
	 * @param pid
	 *            an ID indicating the page to flush
	 */
	private synchronized void flushPage(PageId pid) throws IOException {
		// some code goes here

		Page page = bufferPool.get(pid);
		if (page == null)
			return;
		if (page.isPageDirty() != null) {
			int tableId = page.getId().getTableId();
			DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
			dbFile.writePage(page);
			page.markPageDirty(false, null);
		}

	}

	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2

		Iterator<PageId> iter = bufferPool.keySet().iterator();
		PageId pid = null;
		while (iter.hasNext()) {
			pid = iter.next();
			Page page = bufferPool.get(pid);
			if (tid.equals(page.isPageDirty())) {
				int tableId = page.getId().getTableId();
				DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
				dbFile.writePage(page);
				page.markPageDirty(false, null);
			}
		}

	}

	/**
	 * Discards a page from the buffer pool. Flushes the page to disk to ensure
	 * dirty pages are updated on disk.
	 */
	private synchronized void evictPage() throws DbException {
		// some code goes here
		// not necessary for lab1

		Iterator<PageId> iter = bufferPool.keySet().iterator();
		PageId pid = null;
		Page page = null;
		while (iter.hasNext()) {
			pid = iter.next();
			page = bufferPool.get(pid);
			if (page.isPageDirty() == null) {
				break;
			}
			pid = null;
		}
		if (pid == null) {
			throw new DbException("Every page is dirty.");
		}

		try {
			flushPage(pid);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		bufferPool.remove(pid);
	}

}
