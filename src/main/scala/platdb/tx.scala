package platdb

import scala.collection.mutable.{Map,ArrayBuffer}
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.Breaks._

/*
A: 
    Reads and writes to transactions occur in memory, so if a transaction fails before committing, it does not affect the contents of the disk
    All additions, deletions, and changes to the DB by read-write transactions will be recorded on the dirty pages, 
    and the freelist is written to the file first when the transaction is committed, then the dirty page, and finally the meta information is committed
    The meta information contains the current DB version and root node information, 
    and only the successful writing of meta indicates that the transaction is committed successfully, 
    so if the transaction fails before writing meta, only writing dirty pages or freelist will not affect the consistency of db content
    So how to guarantee atomicity if writing meta error? ———> boltdb does this
C:
    Write transactions are performed serially, 
    so each open read or write transaction is operating on a DB object that is an updated version of the previous write transaction.
    Due to the atomicity/durability guarantee, as long as each write transaction itself is logically consistent, 
    it drives the db view to move from one consistent state to the next
I:
    Multiple read-only transactions and at most one read-write transaction are allowed to operate the database at the same time,
    and MVCC is used to ensure the isolation of concurrent transactions
    When a transaction is opened, it will copy the latest meta information, that is, it holds the latest version of the DB, 
    so the later opened transaction will not read the content of the old version
    The read-only transaction does not change the DB version, and the write transaction will update the version information of the DB after the commit is successful,
    because the changes of the write transaction to the DB are written to the dirty page, 
    so other parallel read-only transactions during the execution of the read and write transaction will not read the modification of the write transaction
    (Pages freed by write transactions can only be used for allocation as dirty pages if no other read-only transactions are held.)
D:
    Modifications to the DB by a successfully committed write transaction are persisted to the disk file,
    and an uncommitted transaction does not affect the contents of the DB
*/

trait Transaction:
    // return current transactions identity
    def id:Int
    // readonly or read-write transaction
    def writable:Boolean
    // if the transaction is closed
    def closed:Boolean
    // commit transaction,if its already closed then throw an exception
    def commit():Try[Boolean]
    // rollback transaction,if its already closed then throw an exception
    def rollback():Try[Boolean]
    // open a bucket,if not exists will throw an exception
    def openBucket(name:String):Try[Bucket]
    // create a bucket,if already exists will throw an exception
    def createBucket(name:String):Try[Bucket]
    // create a bucket,if exists then return the bucket
    def createBucketIfNotExists(name:String):Try[Bucket]
    // delete a bucket,if bucket not exists will throw an exception
    def deleteBucket(name:String):Try[Boolean]

// platdb transaction implement
private[platdb] class Tx(val readonly:Boolean) extends Transaction:
    var sysCommit:Boolean = false 
    var db:DB = null
    var meta:Meta = null
    var root:Bucket = null
    var blocks:Map[Int,Block] = null // cache dirty blocks,rebalance(merge/split) bucket maybe product them
    
    private[platdb] def init(db:DB):Unit =
        this.db = db
        meta = db.meta.clone
        blocks = Map[Int,Block]()
        root = new Bucket("",this)
        root.bkv = meta.root.clone
        if !readonly then
            meta.txid+=1

    def id:Int = meta.txid
    def closed:Boolean = db == null
    def writable:Boolean = !readonly
    private[platdb] def rootBucket():Option[Bucket] = Some(root)
    // open and return a bucket
    def openBucket(name:String):Try[Bucket] = root.getBucket(name)
    // craete a bucket
    def createBucket(name:String):Try[Bucket] = root.createBucket(name)
    // 
    def createBucketIfNotExists(name:String):Try[Bucket] = root.createBucketIfNotExists(name)
    // delete bucket
    def deleteBucket(name:String):Try[Boolean] = root.deleteBucket(name)
    // commit current transaction
    def commit():Try[Boolean] = 
        if closed then
            return Failure(new Exception("tx closed"))
        else if db.isClosed then // TODO remove this
            return Failure(new Exception("db closed"))
        else if sysCommit then
            return Failure( new Exception("not allow to rollback system tx manually"))
        else if !writable then // cannot commit read-only tx
            return Failure(new Exception("cannot commit read-only tx"))
        
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
        Success(true)
    
    //  rollback current tx during commit process
    private[platdb] def rollbackTx():Try[Boolean] = 
        if closed then 
            return Failure(throw new Exception("tx closed"))
        else if db.isClosed then
            return Failure(throw new Exception("db closed"))
        //
        if writable then
            db.freelist.rollback(id)
            // TODO:
            // Read free page list from freelist page.
			// tx.db.freelist.reload(tx.db.page(tx.db.meta().freelist))
        close()
        Success(true)
    // user call rollback directly，because not write any change to db file, so we do nothing except rollback freelist.
    def rollback():Try[Boolean] =
        if closed then
            return Failure(new Exception("tx closed"))
        else if db.isClosed then
            return Failure(new Exception("db closed"))
        else if sysCommit then
            return Failure(new Exception("not allow to rollback system tx manually"))
        if writable then
            db.freelist.rollback(id)
        close()
        Success(true)
    // close current tx: release all object references about the tx 
    private def close():Unit = 
        if closed then 
            return None 
        else if db.isClosed then 
            return None 
        
        if !writable then
            db.removeTx(id)
        else 
            db.removeRTx()

        for (id,bk)<- blocks do
            db.blockBuffer.revert(bk.id)
        db = null
        meta = null
        root = new Bucket("",this)
        blocks.clear()

    // write freelist to db file
    private def writeFreelist():Unit = 
        // allocate new pages for freelist
        val tailId = meta.pageId
        val sz = db.freelist.size()
        val pgid = allocate(sz)
        
        var bk = db.blockBuffer.get(sz)
        bk.setid(pgid)
        db.freelist.writeTo(bk)
        if meta.pageId > tailId then
            db.growTo((meta.pageId+1)*osPageSize) match
                case Success(_) => None
                case Failure(e) => throw new Exception(s"write freelist failed:${e.getMessage()}")
        // write new freelsit to file
        db.blockBuffer.write(bk) match
            case Failure(e) => throw e
            case Success(flag) =>
                if !flag then
                    //rollbackTx()
                    throw new Exception(s"tx ${id} write freelist to db file failed")
                meta.freelistId = id
                db.blockBuffer.revert(bk.id)
    // write all dirty blocks to db file
    private def writeBlock():Unit =
        var arr = new ArrayBuffer[Block]()
        for (id,bk) <- blocks do
            arr+=bk 
        arr.sortWith((b1:Block,b2:Block) => b1.id < b2.id)
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
            for (_,bk) <- blocks do
                db.blockBuffer.revert(bk.id)
 
    // write meta blocks to db file
    private def writeMeta():Unit  =
        var bk = db.blockBuffer.get(meta.size())
        bk.setid(meta.id)
        try
            db.blockBuffer.write(bk) match
                case Failure(e) => throw e
                case Success(flag) =>
                    if !flag then
                        throw new Exception(s"tx ${id} commit meta ${bk.id} failed")
        catch
            case e:Exception => throw e
        finally
            db.blockBuffer.revert(bk.id)
    // return the max pageid
    private[platdb] def maxPageId:Int = meta.pageId
    // get block by bid
    private[platdb] def block(id:Int):Try[Block] =
        if blocks.contains(id) then 
            blocks.get(id) match
                case Some(bk) =>  return Success(bk)
                case None => return Failure(new Exception(s"null block cache $id"))
        db.blockBuffer.read(id) match
            case Success(bk) => 
                blocks(id) = bk 
                Success(bk)
            case Failure(e) => Failure(e)
        
    // release a block's pages
    private[platdb] def free(id:Int):Unit =
        block(id) match
            case Failure(e) => None 
            case Success(bk) => 
                db.freelist.free(id,bk.header.pgid,bk.header.overflow)
    // allocate page space according size 
    private[platdb] def allocate(size:Int):Int =
        var n = size/osPageSize
        if size%osPageSize!=0 then n+=1
        // try to allocate space from frreelist
        var pgid = db.freelist.allocate(id,n)
        // if freelist not have space,we need allocate from db tail and grow the db file.
        if pgid < 0 then
            pgid = meta.pageId
            meta.pageId+=n
        pgid
    
    // construct a block object according size, then set its id
    private[platdb] def makeBlock(id:Int,size:Int):Block = 
        var bk = db.blockBuffer.get(size)
        bk.setid(id)
        blocks.addOne((id,bk))
        bk 
