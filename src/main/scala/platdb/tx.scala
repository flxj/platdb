package platdb

import scala.collection.mutable.{Map,ArrayBuffer}
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/*
A: 
    事务的读写操作都发生在内存中，因此如果事务在提交之前失败，不会影响磁盘中的内容
    读写事务对db的所有增删改操作都会记录到dirty page上，提交事务时候先将freelist写入文件，然后是dirty page,最后提交meta信息
    meta信息包含当前db的版本以及根节点等信息，只有meta成功写入才表示事务提交成功，因此若事务在写meta之前失败，此时只写入dirty pages或者freelist均不会影响db内容的一致性
    那么如果写meta错误如何保证原子性呢？———> boltdb这样做
C:
    写事务是串行执行的，因此每个打开的读写事务操作的db对象均是上一个唯一的写事务更新后的版本，
    由于原子性/持久性的保证，只要每个读写事务的操作是一致的，db视图就会从一个一致状态移动到下一个一致状态
I:
    允许同时有多个只读事务以及至多一个读写事务同时操作数据库，并使用MVCC保证并发事务的隔离性
    当开启一个事务，会拷贝一份当前最新的meta信息，即持有一个db的最新版本，因此后开启的事务不会读到旧版本的内容
    只读事务不会改变db版本，读写事务会在提交成功后更新db的版本信息，因为读写事务对db的变动都写入dirty page, 因此读写事务执行期间其他并发执行的只读事务不会读到该写事务的修改
    （读写事务释放的page只有在没有其他任何只读事务持有后才能用于分配为dirty page)
D:
    提交成功的读写事务对db的修改均会持久化到磁盘文件，未提交成功的事务不会影响db的内容

*/

class Tx(val readonly:Boolean):
    private[platdb] var sysCommit:Boolean
    private[platdb] var db:DB = null
    private[platdb] var meta:Meta = null
    private[platdb] var root:Bucket
    private[platdb] var blocks:Map[Int,Block] = null // cache dirty blocks,rebalance(merge/split) bucket maybe product them
    
    private[platdb] def closed:Boolean = db == null

    private[platdb] def init(db:DB):Unit =
        db = db
        meta = db.meta.clone
        blocks = new Map[Int,Block]()
        root = new Bucket("",this)
        root.bkv = meta.root.clone
        if !readonly then
            meta.txid++

    def txid:Int = meta.txid
    def writable:Boolean = !readonly
    def rootBucket():Option[Bucket] = Some(root)
    // open and return a bucket
    def openBucket(name:String):Option[Bucket] = root.getBucket(name)
    // craete a bucket
    def createBucket(name:String):Option[Bucket] = root.createBucket(name)
    // 
    def createBucketIfNotExists(name:String):Option[Bucket] = root.createBucketIfNotExists(name)
    // delete bucket
    def deleteBucket(name:String):Boolean = root.deleteBucket(name)
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
                    case Failure(e) => _
                return Failure(e)
        
        // updata meta info
        meta.root = root.bkv

        // free the old freelist and write the new list to db file.
        if meta.freelistId!=0 then
            db.freelist.free(txid,db.freelist.header.pgid,db.freelist.header.overflow)
        // 
        writeFreelist() match
            case Failure(e) => return Failure(e)
        writeBlock() match
            case Failure(e) => 
                rollbackTx()
                return Failure(e)
        writeMeta() match
            case Failure(e) => 
                rollbackTx()
                return Failure(e)
        close()
        Success(true)
    
    //  rollback current tx during commit process
    private[platdb] def rollbackTx():Unit = 
        if closed then 
            throw new Exception("tx closed")
        else if db.isClosed then
            throw new Exception("db closed")
        //
        if writable then
            db.freelist.rollback(txid)
            // TODO:
            // Read free page list from freelist page.
			// tx.db.freelist.reload(tx.db.page(tx.db.meta().freelist))
        close()
    // user call rollback directly，because not write any change to db file, so we do nothing except rollback freelist.
    def rollback():Try[Boolean] =
        if closed then
            return Failure(new Exception("tx closed"))
        else if db.isClosed then
            return Failure(new Exception("db closed"))
        else if sysCommit then
            return Failure(new Exception("not allow to rollback system tx manually"))
        if writable then
            db.freelist.rollback(txid)
        close()
        Success(true)
    // close current tx: release all object references about the tx 
    private def close():Unit = 
        if closed then 
            return None 
        else if db.isClosed then 
            return None 
        
        if !writable then
            db.removeTx(txid)
        else 
            db.rwTx = None 
		    db.rwLock.writeLock().unlock()

        for (id,bk)<- blocks do
            db.blockBuffer.revert(bk.uid)
        db = null
        meta = null
        root = new Bucket("",this)
        blocks.clear()

    // write freelist to db file
    private def writeFreelist():Try[Boolean] = 
        // allocate new pages for freelist
        val maxId = meta.pageId
        val sz = db.freelist.size
        allocate(sz) match
            case None => 
                rollbackTx()
                return Failure(new Exception(s"tx ${txid} allocate block space failed"))
            case Some(id) =>
                var bk = db.blockBuffer.get(sz)
                bk.setid(id)
                db.freelist.writeTo(bk)
                // write new freelsit to file
                db.blockBuffer.write(bk) match
                    case Failure(e) => return Failure(e)
                    case Success(flag) =>
                        if !flag then
                            rollbackTx()
                            return Failure(new Exception(s"tx ${txid} write freelist to db file failed"))
                        meta.freelistId = id 
                        db.blockBuffer.revert(bk.uid)
                        /*
                        // If the high water mark has moved up then attempt to grow the database.
                        if tx.meta.pgid > opgid {
                            if err := tx.db.grow(int(tx.meta.pgid+1) * tx.db.pageSize); err != nil {
                                tx.rollback()
                                return err
                            }
                        } 
                        */
                        if meta.pageId > maxId then
                            // TODO grow
                        Success(true)
    // write all dirty blocks to db file
    private def writeBlock():Try[Boolean] =
        var arr = new ArrayBuffer[Block]()
        for (id,bk) <- blocks do
            arr+=bk 
        arr.sortWith((b1:Block,b2:Block) => b1.id < b2.id)

        for bk <- arr do
            db.blockBuffer.write(bk) match
                case Failure(e) => return Failure(e)
                case Success(flag) =>
                    if !flag then
                        return Failure(new Exception(s"tx ${txid} commit dirty blcok ${bk.id} failed"))
        Success(true) 
    // write meta blocks to db file
    private def writeMeta():Try[Boolean]  =
        var bk = db.blockBuffer.get(meta.size)
        bk.setid(meta.id)
        try 
            db.blockBuffer.write(bk) match
                case Failure(e) => return Failure(e)
                case Success(flag) =>
                    if !falg then
                        return Failure( new Exception(s"tx ${txid} commit meta ${bk.id} failed"))
            Success(true)
        finally
            db.blockBuffer.revert(bk.uid)
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
            case None => None 
            case Some(bk) => 
                db.freelist.free(txid,bk.header.pgid,bk.header.overflow)
    // allocate page space according size 
    private[platdb] def allocate(size:Int):Option[Int] =
        var n = size/osPageSize
        if size%osPageSize!=0 then n++
        db.freelist.allocate(txid,n)
    
    // construct a block object according size, then set its id
    private[platdb] def makeBlock(id:Int,size:Int):Block = 
        var bk = db.blockBuffer.get(size)
        bk.setid(id)
        blocks.addOne((id,bk))
        bk 
