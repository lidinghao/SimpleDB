package simpledb;

import java.io.*;
import java.nio.ByteBuffer;
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
    private File file;
    private TupleDesc td;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file =f;
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
        int pageno = pid.pageNumber();
        HeapPageId heapPid = (HeapPageId) pid;
        if(pageno < numPages()) {
            int pageSize = BufferPool.PAGE_SIZE;
            int startPos = pageno * pageSize;
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw")){
                byte[]  buf  = new byte[pageSize];
                ByteBuffer buffer = ByteBuffer.allocate(pageSize);
                raf.getChannel().read(buffer,startPos);
                //int size = raf.read(buf,startPos,pageSize);
//                long pos = startPos ;
//                MappedByteBuffer buffer = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, startPos, pageSize);
//                buffer.load();
//                buffer.get(buf,0,pageSize);
                return new HeapPage(heapPid,buffer.array());

            }catch(IOException x){

            }
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(file.length() / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here

        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        final TransactionId transId = tid;
        return new DbFileIterator() {
            Page currentPage;
            Page savedPos;
            Iterator<Tuple> currentItr;
            BufferPool bufPool= Database.getBufferPool();
            private HeapPage nextPage() throws DbException, TransactionAbortedException {

                HeapPage nextPage;
                int tableId = currentPage.getId().getTableId();
                int pno = currentPage.getId().pageNumber();
                pno++;
                if (pno >= numPages())
                    return null;
                PageId pid = new HeapPageId(tableId,pno);
                nextPage = (HeapPage)bufPool.getPage(transId, pid, Permissions.READ_ONLY);
                return nextPage;
            }
            @Override
            public void open() throws DbException, TransactionAbortedException {
                PageId pid = new HeapPageId(getId(),0);
                currentPage =  bufPool.getPage(transId, pid, Permissions.READ_WRITE);
                currentItr = ((HeapPage)currentPage).iterator();
                savedPos = currentPage;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (currentPage == null)
                    return false;
                HeapPage page = (HeapPage) currentPage;
                Iterator<Tuple> itr = currentItr;
                do{
                    if (itr.hasNext())
                        return itr.hasNext();
                    else {
                        page = nextPage();
                        itr = page == null ? null : page.iterator();

                    }
                }while (page!= null);
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {

                if(currentPage == null)
                    throw  new NoSuchElementException("");

                while (currentPage != null && currentPage.getId().pageNumber() < numPages() ){
                    if ( currentItr.hasNext())
                        return currentItr.next();
                    else {
                        currentPage = nextPage();
                        currentItr = ((HeapPage) currentPage).iterator();
                    }
                }
                return null;
            }
            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if(currentPage == null)
                    throw  new DbException("");
                     currentPage  = savedPos;
            }

            @Override
            public void close() {
                currentPage = null;
                currentItr = null;

            }
        };


    }

}

