package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     * the file that stores the on-disk backing store for this heap
     * file.
     */
    private File file;
    private TupleDesc td;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
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
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pgNo = pid.pageNumber();
        HeapPageId heapPid = (HeapPageId) pid;
        if (pgNo < numPages()) {
            int pageSize = BufferPool.PAGE_SIZE;
            int offset = pgNo * pageSize;
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
//                ByteBuffer buffer = ByteBuffer.allocate(pageSize);
//                raf.getChannel().read(buffer, offset);
                byte[] buffer = new byte[pageSize];
                raf.seek(offset);
                raf.read(buffer, 0, pageSize);
                return new HeapPage(heapPid, buffer);

            } catch (IOException x) {

            }
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
        // some code goes here
        PageId pid = page.getId();
        int pageno = pid.pageNumber();
        HeapPageId heapPid = (HeapPageId) pid;
        int pageSize = BufferPool.PAGE_SIZE;
        int offset = pageno * pageSize;
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
//            ByteBuffer buffer = ByteBuffer.wrap(page.getPageData());
//            raf.getChannel().write(buffer, startPos);
            byte[] buffer = new byte[pageSize];
            raf.seek(offset);
            raf.write(buffer, 0, pageSize);
            raf.close();
        } catch (IOException x) {

        }

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int num = 0;

            num =  (int) Math.ceil(file.length() / BufferPool.PAGE_SIZE);

        return  num;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        HeapPageId pid = getFreePage(tid).getId();
        HeapPage page = (HeapPage)Database
                .getBufferPool()
                .getPage(tid, pid,Permissions.READ_WRITE);

        page.insertTuple(t);
        ArrayList<Page> arrayList = new ArrayList<>();
        arrayList.add(page);
        return arrayList;
        // not necessary for proj1
    }

    /**
     *
     * @param tid the
     * @return
     * @throws TransactionAbortedException
     * @throws DbException
     * @throws IOException
     */
    private HeapPage getFreePage(TransactionId tid) throws TransactionAbortedException, DbException, IOException {
        BufferPool buf = Database.getBufferPool();
        for (int i = 0; i < numPages(); i++) {
            PageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) buf.getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                 return page;
            }

        }
        HeapPageId pid = new HeapPageId(getId(), numPages());
        HeapPage page = new HeapPage(pid, new byte[BufferPool.PAGE_SIZE]);
        writePage(page); // append this new page to the DbFile
        return page;


    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        PageId pageId = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);
        return page;
        // not necessary for proj1
    }

    class HeapFileIterator implements DbFileIterator {
        private boolean isOpened = false;
        private TransactionId tid;
        private HeapFile file;
        private int currentPage;
        private Iterator<Tuple> pageIter;
        private int tableId;

        public HeapFileIterator(TransactionId tid, HeapFile file) {
            this.tid = tid;
            this.file = file;
            tableId = file.getId();
        }

        @Override

        public void open() throws DbException, TransactionAbortedException {
            if (!isOpened) {
                currentPage = 0;
                pageIter = getPageIterator(currentPage);
            }

        }

        private Iterator<Tuple> getPageIterator(int currentPage) throws TransactionAbortedException, DbException {
            HeapPageId pid = new HeapPageId(tableId, currentPage);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (pageIter == null) {
                return false;
            } else if (pageIter.hasNext())
                return true;

            if (currentPage >= file.numPages()) {
                return false;
            } else {
                int pgNo = currentPage;
                pgNo++;
                while (pgNo < file.numPages()) {
                    Iterator<Tuple> iter = getPageIterator(pgNo);
                    if (iter.hasNext()) return true;
                    pgNo++;
                }
                return false;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext())
                throw new NoSuchElementException("no tuple");
            else {
                if (pageIter.hasNext())
                    return pageIter.next();
                else {
                    currentPage++;
                    while (currentPage < file.numPages()) {
                        pageIter = getPageIterator(currentPage);
                        if (pageIter.hasNext()) return pageIter.next();
                        currentPage++;
                    }

                }

            }
            throw new NoSuchElementException("no tuple");
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();

        }

        @Override
        public void close() {
            isOpened = false;
            pageIter = null;

        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, this);
//        final TransactionId transId = tid;
//        return new DbFileIterator() {
//            Page currentPage;
//            Page savedPos;
//            Iterator<Tuple> currentItr;
//            BufferPool bufPool= Database.getBufferPool();
//
//            private HeapPage nextPage() throws DbException, TransactionAbortedException {
//
//                HeapPage nextPage;
//                int tableId = currentPage.getId().getTableId();
//                int pno = currentPage.getId().pageNumber();
//                pno++;
//                if (pno >= numPages())
//                    return null;
//                PageId pid = new HeapPageId(tableId,pno);
//                nextPage = (HeapPage)bufPool.getPage(transId, pid, Permissions.READ_ONLY);
//
//                return nextPage;
//            }
//            @Override
//            public void open() throws DbException, TransactionAbortedException {
//                PageId pid = new HeapPageId(getId(),0);
//                currentPage =  bufPool.getPage(transId, pid, Permissions.READ_WRITE);
//                currentItr = ((HeapPage)currentPage).iterator();
//                savedPos = currentPage;
//            }
//
//            @Override
//            public boolean hasNext() throws DbException, TransactionAbortedException {
//                if (currentPage == null)
//                    return false;
//                int pno = currentPage.getId().pageNumber();
//                int tableId = currentPage.getId().getTableId();
//                HeapPage page = (HeapPage) currentPage;
//                Iterator<Tuple> itr = currentItr;
//                do{
//                    if (itr.hasNext())
//                        return itr.hasNext();
//                    else {
//                        pno++;
//                        if (pno >= numPages())
//                            return false;
//                        PageId pid = new HeapPageId(tableId,pno);
//                        page = (HeapPage)bufPool.getPage(transId, pid, Permissions.READ_ONLY);
//                        itr = page.iterator();
//
//                    }
//                }while (page!= null);
//                return false;
//            }
//
//            @Override
//            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
//
//                if(currentPage == null)
//                    throw  new NoSuchElementException("");
//
//                while (currentPage != null && currentPage.getId().pageNumber() < numPages() ){
//                    if ( currentItr.hasNext())
//                        return currentItr.next();
//                    else {
//                        currentPage = nextPage();
//                        currentItr = ((HeapPage) currentPage).iterator();
//                    }
//                }
//                return null;
//            }
//            @Override
//            public void rewind() throws DbException, TransactionAbortedException {
//                if(currentPage == null)
//                    throw  new DbException("");
//                     currentPage  = savedPos;
//            }
//
//            @Override
//            public void close() {
//                currentPage = null;
//                currentItr = null;
//
//            }
//        };


    }

}

