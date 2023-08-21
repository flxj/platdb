package platdb

import scala.collection.mutable.{Map,ArrayBuffer}

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
    private[platdb] var db:DB
    private[platdb] var meta:Meta = _ 
    private[platdb] var root:Bucket = _ 
    private[platdb] var blocks:Map[Int,Block] = _  // cache dirty blocks,rebalance(merge/split) bucket maybe product them
    private[platdb] var closed:Boolean

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
    def commit():Boolean = 
        if db.closed then
            return false 
        else if closed then
            return false 
        else if !writable then // cannot commit read-only tx
            return false  
        // merge bucket nodes first
        root.merge()
        // spill buckets to dirty blocks
        if !root.split() then
            rollback()
            return false 
        
        // updata meta info
        meta.root = root.bkv

        // free the old freelist and write the new list to db file.
        if meta.freelistId!=0 then
            db.freelist.free(txid,db.freelist.header.pgid,db.freelist.header.overflow)
        if !writeFreelist() then
            return false 
        
        if !writeBlock() then
            rollbackTx()
            return false 
        
        if !writeMeta() then
            rollbackTx()
            return false 
        close()
        true 
    //  rollback current tx
    private def rollbackTx():Unit = 
        if db.closed then
            return None 
        else if closed then
            return None 
        if writable then
            db.freelist.rollbackFree(txid)
            // TODO:
            // Read free page list from freelist page.
			// tx.db.freelist.reload(tx.db.page(tx.db.meta().freelist))
        close()
    // user call rollback directly，because not write any change to db file, so we do nothing except rollback freelist.
    def rollback():Unit =
        if db.closed then
            return None 
        else if closed then
            return None 
        if writable then
            db.freelist.rollback(txid)
        close()
    // close current tx: release all object references about the tx 
    private def close():Unit = 
        if db.closed then 
            return None 
        else if closed then 
            return None 
        
        if !writable then
            db.removeTx(txid)
        else 
            db.rwTx = None 
		    db.rwLock.writeLock().unlock()

        for (id,bk)<- blocks do
            db.blockBuffer.revert(bk.uid)
        db = None 
        meta = None
        root = new Bucket("",this)
        blocks.clear()

    // write freelist to db file
    private def writeFreelist():Boolean = 
        // allocate new pages for freelist
        val maxId = meta.blockId
        val sz = db.freelist.size
        allocate(sz) match
            case None => 
                rollbackTx()
                false 
            case Some(id) =>
                var bk = db.blockBuffer.get(sz)
                bk.setid(id)
                db.freelist.writeTo(bk)
                // write new freelsit to file
                val (success,_) = db.blockBuffer.write(bk)
                if !success then
                    rollbackTx()
                else
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
                if success && meta.blockId > maxId then
                    // TODO
                success
    // write all dirty blocks to db file
    private def writeBlock():Boolean =
        var arr = new ArrayBuffer[Block]()
        for (id,bk) <- blocks do
            arr+=bk 
        arr.sortWith((b1:Block,b2:Block) => b1.id < b2.id)

        for bk <- arr do
            val (success,_) = db.blockBuffer.write(bk)
            if !success then
                return false 
        true 
    // write meta blocks to db file
    private def writeMeta():Boolean  =
        var bk = db.blockBuffer.get(meta.size)
        bk.setid(meta.id)

        val (success,_) = db.blockBuffer.write(bk)
        db.blockBuffer.revert(bk.uid)
        success
    
    // return the max blockid
    private[platdb] def blockId:Int = meta.blockId
    // get block by bid
    private[platdb] def block(id:Int):Option[Block] =
        if blocks.contains(id) then 
            return blocks.get(id)
        db.blockBuffer.read(id) match
            case Some(bk) => 
                blocks(id) = bk 
                return Some(bk)
            case None => return None 
        
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
