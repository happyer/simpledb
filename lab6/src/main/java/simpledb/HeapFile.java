package simpledb;


import java.io.*;
import java.util.*;


/**
 * heap 实现的Db File
 */
public class HeapFile implements DbFile {

    private final File dbFile;
    private final TupleDesc tupleDesc;

    /**
     * construction
     *
     * @param f
     * @param td
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.dbFile = f;
        this.tupleDesc = td;
    }


    /**
     * 返回 heap file
     *
     * @return
     */
    public File getFile() {

        return dbFile;
    }


    /**
     * 返回一个id,每次都返回同样的一个 id,这里简单一点直接返回绝对路径的 hash code
     *
     * @return
     */
    public int getId() {
        return dbFile.getAbsoluteFile().hashCode();
    }

    /**
     * 返回描述符
     *
     * @return
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * 读取db file 里面的一页数据
     * pageId，里面获取 读取第几页pageNo
     * 通过buffer pool 获得 每一页的大小 pageSize
     * 读取 pageNo * pageSize 之后的数据，因为我们是分页读取的，所以需要skip 掉
     * pageNo * pageSize
     *
     * @param pid
     * @return
     */
    public Page readPage(PageId pid) {

        int tableid = pid.getTableId();
        int pgNo = pid.pageNumber();
        final int pageSize = Database.getBufferPool().getPageSize();
        byte[] rawPgData = HeapPage.createEmptyPageData();

        // random access read from disk
        FileInputStream in = null;
        try {
            in = new FileInputStream(dbFile);
            try {
                in.skip(pgNo * pageSize);
                in.read(rawPgData);
                return new HeapPage(new HeapPageId(tableid, pgNo), rawPgData);
            } catch (IOException e) {
                throw new IllegalArgumentException("HeapFile: readPage:");
            } finally {
                in.close();
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("HeapFile: readPage: file not found");
        }

    }

    /**
     * 这里和读取类似，需要做一个分页处理，写在特定的位置上面
     *
     * @param page
     * @throws IOException
     */
    public void writePage(Page page) throws IOException {
        PageId pid = page.getId();
        int pgNo = pid.pageNumber();

        final int pageSize = Database.getBufferPool().getPageSize();
        byte[] pgData = page.getPageData();

        RandomAccessFile dbfile = new RandomAccessFile(dbFile, "rws");
        dbfile.skipBytes(pgNo * pageSize);
        dbfile.write(pgData);
    }


    /**
     * 返回该文件的多少页
     *
     * @return
     */
    public int numPages() {
        // some code goes here
        int fileSizeinByte = (int) dbFile.length();
        return fileSizeinByte / Database.getBufferPool().getPageSize();
    }

    /**
     * 在一个事务里面，添加一个tuple
     * 首先我们去 buffer pool 里面寻找一个page ,如果找不到
     * page 我们就新建一个page
     * 然后在page里面插入 tuple
     * 如果我们的page 还么有溢出那么就需要记录起来，通过list 记录起来，等待后面进行flush 刷新
     * 如果超过我们现在buffer 了，则直接进行append，到file 里面
     *
     * @return
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> affected = new ArrayList(1);
        int numPages = numPages();

        for (int pgNo = 0; pgNo <= numPages; pgNo++) {
            HeapPageId pid = new HeapPageId(getId(), pgNo);
            HeapPage pg;
            if (pgNo < numPages) {
                pg = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            } else {
                // pgNo = numpages -> we need add new page
                pg = new HeapPage(pid, HeapPage.createEmptyPageData());
            }

            if (pg.getNumEmptySlots() > 0) {
                // insert will update tuple when inserted
                pg.insertTuple(t);
                // writePage(pg);
                if (pgNo < numPages) {
                    pg.markDirty(true, tid);
                    affected.add(pg);
                } else {
                    // should append the dbfile
                    writePage(pg);
                }
                return affected;
            }

        }
        throw new DbException("HeapFile: InsertTuple: Tuple can not be added");
    }

    /**
     * 跟上插入tuple 有一定的类同
     * 只是这里不会新建一个page,同时也需要记录更改过的page,方便后续进行flush 操作
     *
     * @param tid
     * @param t
     * @return
     * @throws DbException
     * @throws TransactionAbortedException
     */
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> affected = new ArrayList(1);
        RecordId rid = t.getRecordId();
        HeapPageId pid = (HeapPageId) rid.getPageId();
        if (pid.getTableId() == getId()) {
            // int pgNo = pid.pageNumber();
            HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            pg.deleteTuple(t);
            // writePage(pg);
            pg.markDirty(true, tid);
            affected.add(pg);
            return affected;
        }
        throw new DbException("HeapFile: deleteTuple: tuple.tableid != getId");
    }


    /**
     * DB File iterator 的迭代器，之前说过，db 的获取是通过 iterator 获取的
     */
    private class HeapFileIterator implements DbFileIterator {

        private Integer pgCursor;
        private Iterator<Tuple> tupleIter;
        private final TransactionId transactionId;
        private final int tableId;
        private final int numPages;

        public HeapFileIterator(TransactionId tid) {
            this.pgCursor = null;
            this.tupleIter = null;
            this.transactionId = tid;
            this.tableId = getId();
            this.numPages = numPages();
        }

        public void open() throws DbException, TransactionAbortedException {
            pgCursor = 0;
            tupleIter = getTupleIter(pgCursor);
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            // < numpage - 1
            if (pgCursor != null) {
                while (pgCursor < numPages - 1) {
                    if (tupleIter.hasNext()) {
                        return true;
                    } else {
                        pgCursor += 1;
                        tupleIter = getTupleIter(pgCursor);
                    }
                }
                return tupleIter.hasNext();
            } else {
                return false;
            }
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext()) {
                return tupleIter.next();
            }
            throw new NoSuchElementException("HeapFileIterator: error: next: no more elemens");
        }

        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        public void close() {
            pgCursor = null;
            tupleIter = null;
        }

        private Iterator<Tuple> getTupleIter(int pgNo)
                throws TransactionAbortedException, DbException {
            PageId pid = new HeapPageId(tableId, pgNo);
            return ((HeapPage)
                    Database.getBufferPool()
                            .getPage(transactionId, pid, Permissions.READ_ONLY)).iterator();
        }
    }

    //
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

}

