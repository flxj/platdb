/*
   Copyright (C) 2023 flxj(https://github.com/flxj)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package platdb

import scala.collection.mutable.{Map,ArrayBuffer}
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.Breaks._
import java.io.RandomAccessFile
import java.io.File

/**
  * platdb transaction.
  */
trait Transaction:
    /**
      * return current transactions identity.
      *
      * @return
      */
    def id:Long
    /**
      * readonly or read-write transaction.
      *
      * @return
      */
    def writable:Boolean
    /**
      * if the transaction is closed.
      *
      * @return
      */
    def closed:Boolean
    /**
      * return db bytes size in current transaction view.
      *
      * @return
      */
    def size:Long
    /**
      * commit transaction,if its already closed then return an exception message.
      *
      * @return
      */
    def commit():Try[Unit]
    /**
      * rollback transaction,if its already closed then return an exception message.
      *
      * @return
      */
    def rollback():Try[Unit]
    /**
      * return all collection objects in current db,format is (name,dataType).
      *
      * @return
      */
    def allCollection():Try[Seq[(String,String)]]
    /**
      * open a bucket,if not exists will return an exception message.
      *
      * @param name
      * @return
      */
    def openBucket(name:String):Try[Bucket]
    /**
      * create a new bucket,if already exists will return an exception message.
      *
      * @param name
      * @return
      */
    def createBucket(name:String):Try[Bucket]
    /**
      * create a bucket,if exists then return the bucket.
      *
      * @param name
      * @return
      */
    def createBucketIfNotExists(name:String):Try[Bucket]
    /**
      * delete a bucket,if bucket not exists will return an exception message.
      *
      * @param name
      * @return
      */
    def deleteBucket(name:String):Try[Unit]
    /**
      * open a set,if not exists will return an exception message.
      *
      * @param name
      * @return
      */
    def openBSet(name:String):Try[BSet]
    /**
      * create a new set,if already exists will return an exception message.
      *
      * @param name
      * @return
      */
    def createBSet(name:String):Try[BSet]
    /**
      * create a set,if exists then return the set.
      *
      * @param name
      * @return
      */
    def createBSetIfNotExists(name:String):Try[BSet]
    /**
      * delete a bucket,if bucket not exists will return an exception message.
      *
      * @param name
      * @return
      */
    def deleteBSet(name:String):Try[Unit]
    /**
      * open a list,if not exists will return an exception message.
      *
      * @param name
      * @return
      */
    def openList(name:String):Try[BList]
    /**
      * create a new list,if already exists will return an exception message.
      *
      * @param name
      * @return
      */
    def createList(name:String):Try[BList]
    /**
      * create a list,if exists then return the list.
      *
      * @param name
      * @return
      */
    def createListIfNotExists(name:String):Try[BList]
    /**
      * delete a list,if bucket not exists will return an exception message.
      *
      * @param name
      * @return
      */
    def deleteList(name:String):Try[Unit]
    /**
      * open a region,if not exists will return an exception message.
      *
      * @param name
      * @return
      */
    def openRegion(name:String):Try[Region]
    /**
      * create a region with a specified dimension, and return an exception message if the object already exists.
      *
      * @param name
      * @param dimension
      * @return
      */
    def createRegion(name:String,dimension:Int):Try[Region]
    /**
      * create a region,if exists then return the region.
      *
      * @param name
      * @param dimension
      * @return
      */
    def createRegionIfNotExists(name:String,dimension:Int):Try[Region]
    /**
      * delete a regison,if not exists will return an exception message.
      *
      * @param name
      * @return
      */
    def deleteRegion(name:String):Try[Unit]
    /**
      * backup the database file in current transaction view to a file.
      *
      * @param path
      * @return
      */
    def copyToFile(path:String):Try[Long] 


private[platdb] object Tx:
    def apply(readonly:Boolean,db:DB):Tx =
        var tx = new Tx(readonly)
        tx.db = db
        tx.meta = db.meta.clone
        tx.root = new BTreeBucket("root",tx)
        tx.root.bkv = tx.meta.root.clone
        if !readonly then
            tx.meta.txid+=1
        println(s"[debug] begin a tx ${tx.id}")
        tx

/*
    Reads and writes to transactions occur in memory, so if a transaction fails before committing, it does not affect the contents of the disk
    All additions, deletions, and changes to the DB by read-write transactions will be recorded on the dirty pages, 
    and the freelist is written to the file first when the transaction is committed, then the dirty page, and finally the meta information is committed
    The meta information contains the current DB version and root node information, 
    and only the successful writing of meta indicates that the transaction is committed successfully, 
    so if the transaction fails before writing meta, only writing dirty pages or freelist will not affect the consistency of db content.

    Write transactions are performed serially, 
    so each open read or write transaction is operating on a DB object that is an updated version of the previous write transaction.
    Due to the atomicity/durability guarantee, as long as each write transaction itself is logically consistent, 
    it drives the db view to move from one consistent state to the next.

    Multiple read-only transactions and at most one read-write transaction are allowed to operate the database at the same time,
    and MVCC is used to ensure the isolation of concurrent transactions
    When a transaction is opened, it will copy the latest meta information, that is, it holds the latest version of the DB, 
    so the later opened transaction will not read the content of the old version
    The read-only transaction does not change the DB version, and the write transaction will update the version information of the DB after the commit is successful,
    because the changes of the write transaction to the DB are written to the dirty page, 
    so other parallel read-only transactions during the execution of the read and write transaction will not read the modification of the write transaction
    (Pages freed by write transactions can only be used for allocation as dirty pages if no other read-only transactions are held.)

    Modifications to the DB by a successfully committed write transaction are persisted to the disk file,
    and an uncommitted transaction does not affect the contents of the DB.
*/
private[platdb] class Tx(val readonly:Boolean) extends Transaction:
    var sysCommit:Boolean = false 
    var db:DB = null
    var meta:Meta = null
    var root:BTreeBucket = null
    // cache dirty blocks,rebalance(merge/split) bucket maybe product them.
    var blocks:Map[Long,Block] = Map[Long,Block]()

    def id:Long = meta.txid
    def closed:Boolean = db == null
    def writable:Boolean = !readonly
    // return the max pageid
    def maxPageId:Long = meta.pageId
    //
    def size:Long =  meta.pageId * DB.pageSize
    //
    def rootBucket():Option[Bucket] = Some(root)
    // open and return a bucket
    def openBucket(name:String):Try[Bucket] = root.getBucket(name)
    // craete a bucket
    def createBucket(name:String):Try[Bucket] = root.createBucket(name)
    // 
    def createBucketIfNotExists(name:String):Try[Bucket] = root.createBucketIfNotExists(name)
    // delete bucket
    def deleteBucket(name:String):Try[Unit] = root.deleteBucket(name)
    // commit current transaction
    def commit():Try[Unit] = 
        if closed then
            return Failure(DB.exceptionTxClosed)
        else if db.closed then
            return Failure(DB.exceptionDBClosed)
        else if sysCommit then
            return Failure(DB.exceptionNotAllowCommitSysTx)
        else if !writable then
            return Failure(DB.exceptionNotAllowCommitRTx)
        
        try 
            // merge bucket nodes first
            root.merge()
            // spill buckets to dirty blocks
            root.split()
        catch
            case e:Exception =>
                rollback() match
                    case _ => None
                return Failure(e)
        
        // updata meta info
        meta.root = root.bkv
        // free the old freelist and write the new list to db file.
        if meta.freelistId!=0 then
            db.freelist.free(id,db.freelist.header.pgid,db.freelist.header.overflow)
        //
        try 
            writeFreelist() 
            writeBlock()
            writeMeta() 
        catch
            case e:Exception => 
                rollbackTx() match
                    case _ => None
                return Failure(e)
        close()
        Success(None)
    
    /**
      * rollback current tx during commit process.
      *
      * @return
      */
    private[platdb] def rollbackTx():Try[Boolean] = 
        if closed then 
            return Failure(DB.exceptionTxClosed)
        else if db.closed then
            return Failure(DB.exceptionDBClosed)
        //
        if writable then
            db.freelist.rollback(id)
            // TODO:
            // Read free page list from freelist page.
			// freelist.reload(db.meta.freelist))
        close()
        Success(true)
    
    /**
      * user call rollback directly，because not write any change to db file, so we do nothing except rollback freelist.
      *
      * @return
      */
    def rollback():Try[Unit] =
        if closed then
            return Failure(DB.exceptionTxClosed)
        else if db.closed then
            return Failure(DB.exceptionDBClosed)
        else if sysCommit then
            return Failure(DB.exceptionNotAllowRollbackSysTx)
        if writable then
            db.freelist.rollback(id)
        close()
        Success(None)

    /**
      * close current tx: release all object references about the tx 
      */
    private def close():Unit = 
        if closed then 
            return None 
        else if db.closed then 
            return None 
        
        if !writable then
            db.removeTx(id)
        else 
            db.removeRTx()

        for (id,bk)<- blocks do
            db.blockBuffer.revert(bk.id)
        val txid = id
        db = null
        meta = null
        root = new BTreeBucket("",this)
        blocks.clear()
        println(s"[debug] tx ${txid} closed")

    /**
      * write freelist to db file.
      */
    private def writeFreelist():Unit = 
        // allocate new pages for freelist.
        val tailId = meta.pageId
        val sz = db.freelist.size()
        val pgid = allocate(sz)
        
        var bk = db.blockBuffer.get(sz)
        bk.setid(pgid)
        db.freelist.writeTo(bk)
        if meta.pageId > tailId then
            db.growTo((meta.pageId+1)*DB.pageSize) match
                case Success(_) => None
                case Failure(e) => throw new Exception(s"write freelist failed:${e.getMessage()}")
        // write new freelsit to file
        db.blockBuffer.write(bk) match
            case Failure(e) => throw e
            case Success(flag) =>
                if !flag then
                    throw new Exception(s"tx ${id} write freelist to db file failed")
                meta.freelistId = bk.id
                db.freelist.setId(bk.id,bk.header.overflow)
                db.blockBuffer.revert(bk.id)
    // write all dirty blocks to db file
    private def writeBlock():Unit =
        var arr = new ArrayBuffer[Block]()
        for (id,bk) <- blocks do
            arr+=bk 
        arr = arr.sortWith((b1:Block,b2:Block) => b1.id < b2.id)
        try
            for bk <- arr do
                db.blockBuffer.write(bk) match
                    case Failure(e) => throw e
                    case Success(flag) =>
                        if !flag then
                            throw new Exception(s"tx ${id} commit dirty blcok ${bk.id} failed")
        catch
            case e:Exception => throw e
        finally
            for bk <- arr do
                db.blockBuffer.revert(bk.id)
 
    /**
      * write meta blocks to db file.
      */
    private def writeMeta():Unit  =
        var bk = db.blockBuffer.get(meta.size())
        bk.setid(meta.id)
        meta.writeHeader(bk)
        val bkdata = bk.header.getBytes()
        val metadata = meta.getMetaBytes()
        meta.checkSum = getCheckSum32(Array.concat(bkdata,metadata))
        meta.writeTo(bk)
        try
            db.blockBuffer.write(bk) match
                case Failure(e) => throw e
                case Success(flag) =>
                    if !flag then
                        throw new Exception(s"tx ${id} commit meta ${bk.id} failed")
                    db.updateMeta(meta)
        catch
            case e:Exception => throw e
        finally
            db.blockBuffer.revert(bk.id)
    /**
      * get block by bid
      *
      * @param pgid
      * @return
      */
    def block(pgid:Long):Try[Block] =
        if blocks.contains(pgid) then 
            blocks.get(pgid) match
                case Some(bk) =>  return Success(bk)
                case None => return Failure(new Exception(s"null block cache $pgid"))
        db.blockBuffer.read(pgid) match
            case Success(bk) => 
                blocks(pgid) = bk 
                Success(bk)
            case Failure(e) => Failure(e)
        
    /**
      * release a block's pages.
      *
      * @param pgid
      */
    def free(pgid:Long):Unit =
        block(pgid) match
            case Failure(e) => None 
            case Success(bk) => 
                db.freelist.free(id,bk.header.pgid,bk.header.overflow)
                db.blockBuffer.revert(pgid)
                blocks.remove(pgid)

    /**
      * allocate page space according size 
      *
      * @param size
      * @return
      */
    def allocate(size:Int):Long =
        var n = size/DB.pageSize
        if size%DB.pageSize!=0 then n+=1
        // try to allocate space from frreelist
        var pgid = db.freelist.allocate(id,n)
        // if freelist not have space,we need allocate from db tail and grow the db file.
        if pgid < 0 then
            pgid = meta.pageId
            meta.pageId+=n
        pgid
    
    /**
      * construct a block object according size, then set its id.
      *
      * @param pgid
      * @param size
      * @return
      */
    def makeBlock(pgid:Long,size:Int):Block = 
        var bk = db.blockBuffer.get(size)
        bk.reset()
        bk.setid(pgid)
        blocks(pgid) = bk
        bk 
    
    /**
      * 
      *
      * @param path
      * @return
      */
    def copyToFile(path:String):Try[Long] = 
        if closed then 
            return Failure(DB.exceptionTxClosed)
        else if db.closed then
            return Failure(DB.exceptionDBClosed)
        var writer:RandomAccessFile = null
        try
            var f = new File(path)
            if !f.exists() then
                f.createNewFile()
            f.setWritable(true)
            // 1. copy meta page
            writer = new RandomAccessFile(f,"rw")
            var bk = db.blockBuffer.get(meta.size())
            var buf = new Array[Byte](DB.pageSize)
            // meta0
            bk.setid(0L)
            meta.writeTo(bk)
            bk.all.copyToArray(buf,0)
            writer.seek(bk.id*DB.pageSize)
            writer.write(buf)
            // meta1
            bk.reset()
            bk.setid(1L)
            val meta1 = meta.clone
            meta1.txid = meta.txid-1
            meta1.writeTo(bk)
            bk.all.copyToArray(buf,0)
            writer.seek(bk.id*DB.pageSize)
            writer.write(buf)
            // 2. copy data page
            val off = 2*DB.pageSize
            db.fileManager.copyToFile(f,off,size-off,off) match
                case Failure(e) => Failure(e)
                case Success(n) => Success(n+off)
        catch
            case e:Exception => Failure(e)
        finally
            if writer != null then 
                writer.close()
    /**
      * 
      *
      * @return
      */
    def allCollection(): Try[Seq[(String, String)]] = root.allCollection()
    // BSet methods
    def openBSet(name:String):Try[BSet] = root.getBSet(name)
    def createBSet(name:String):Try[BSet] = root.createBSet(name)
    def createBSetIfNotExists(name:String):Try[BSet] = root.createBSetIfNotExists(name)
    def deleteBSet(name:String):Try[Unit] = root.deleteBSet(name)

    // BList methods.
    def openList(name:String):Try[BList] = root.getList(name,!writable)
    def createList(name:String):Try[BList] = root.createList(name)
    def createListIfNotExists(name:String):Try[BList] = root.createListIfNotExists(name)
    def deleteList(name:String):Try[Unit] = root.deleteList(name)
    
    // Region
    def openRegion(name:String):Try[Region] = root.getRegion(name)
    //
    def createRegion(name:String,dimension:Int):Try[Region] = root.createRegion(name,dimension)
    //
    def createRegionIfNotExists(name:String,dimension:Int):Try[Region] = root.createRegionIfNotExists(name,dimension)
    //
    def deleteRegion(name:String):Try[Unit] = root.deleteRegion(name)
