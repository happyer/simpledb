
package com.chauncy.simpledb;

import java.util.*;
import java.io.*;

/**
 * The interface for database files on disk. Each table is represented by a
 * single DbFile. DbFiles can fetch pages and iterate through tuples. Each
 * file has a unique id used to store metadata about the table in the Catalog.
 * DbFiles are generally accessed through the buffer pool, rather than directly
 * by operators.
 */
public interface DbFile {
    /**
     * 通过pageId,读取磁盘里面的数据，每次读取一个page
     */
    public Page readPage(PageId id);


    /**
     * 将 一个page 的内容写入到磁盘
     * @param p
     * @throws IOException
     */
    public void writePage(Page p) throws IOException;


    /**
     * 通过事务将一个tuple 插入到 db file 里面，这个是需要获取一个lock，来讲进行报货并发的错误
     * @param tid
     * @param t
     * @return
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException;

    /**
     * 删除指定tuple,在一个事务里面，同理也需要获取一个lock
     * @param tid
     * @param t
     * @return
     * @throws DbException
     * @throws TransactionAbortedException
     */
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException;

    /**
     * 获取一个 iterator,在获取数据的db file 里面，都是通过buffer pool,来获取，
     * buffer pool 则是通过 iterator 来一个一个tuple 进行的获取，所以返回的是一个iterator
     * @param tid
     * @return
     */
    public DbFileIterator iterator(TransactionId tid);



    /**
     * 返回 db file 的id,这个id与 catalog 进行，管理这个到后面会进行实现，先明白他是一个唯一表示服务就行
     * @return
     */
    public int getId();

    /**
     * 返回这个db file 的一个描述信息，即是数据库的字段描述
     * @return
     */
    public TupleDesc getTupleDesc();
}
