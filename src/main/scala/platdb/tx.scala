package platdb

import scala.collection.mutable.{Map,ArrayBuffer}
/*
txid可以表示db的一个版本(版本是保护整个db的)，读事务不会改变db的txid(它只能持有一份该读事务创建时候db最新的已经提交的版本), 写事务成功提交后会将db当前的txid+1, 并且由于写事务对db的增删改操作都是新建dirty page,所以不会影响之前的版本。

写事务提交后可能会释放一些page, 这会在freelist的pending中添加一条记录 'txid: pageids' 表示txid版本的这些pageids需要释放

下一个写事务在开始执行之前会尝试调用freelist释放pending中的page, 只要pending中待释放的版本小于当前打开的只读事务持有的最小版本，那末就可以释放这些pending元素 （表示当前肯定已经没有只读事务在持有这些待释放pages了）

同时当前已经打开的只读事务持有的版本可能跨度较大， 那末对于两个相邻版本之间的版本， 如果已经没有事务在持有它，那末也是可以释放的

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
            db.freelist.free(txid,db.freelist.header.id,db.freelist.header.overflow)
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
    // user call rollback directly
    def rollback():Unit =
        if db.closed then
            return None 
        else if closed then
            return None 
        if writable then
            db.freelist.rollbackFree(txid)
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
            db.blockpool.revert(bk.uid)
        db = None 
        meta = None
        root = new Bucket("",this)
        blocks.clear()

    // write freelist to db file
    private def writeFreelist():Boolean = 
        // allocate new pages
        val maxId = meta.blockId
        val sz = db.freelist.size
        allocate(sz) match
            case None => 
                rollbackTx()
                false 
            case Some(id) =>
                var bk = db.blockpool.get(sz)
                bk.setid(id)
                db.freelist.writeTo(bk)

                val (success,_) = db.filemanager.write(bk)
                if !success then
                    rollbackTx()
                else
                    meta.freelistId = id 
                db.blockpool.revert(bk.uid)
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
            val (success,_) = db.filemanager.write(bk)
            if !success then
                return false 
        true 
    // write meta blocks to db file
    private def writeMeta():Boolean  =
        var bk = db.blockpool.get(meta.size)
        bk.setid(meta.id)

        val (success,_) = db.filemanager.write(bk)
        db.blockpool.revert(bk.uid)
        success
    
    // return the max blockid
    private[platdb] def blockId:Int = meta.blockId
    // get block by bid
    private[platdb] def block(id:Int):Option[Block] =
        if blocks.contains(id) then 
            return blocks.get(id)
        db.filemanager.read(id) match
            case Some(bk) => 
                blocks(id) = bk 
                return Some(bk)
            case None => return None 
        
    // release a block's pages
    private[platdb] def free(id:Int):Unit =
        block(id) match
            case None => None 
            case Some(bk) => 
                db.freelist.free(txid,bk.header.id,bk.header.overflow)
    // allocate page space according size 
    private[platdb] def allocate(size:Int):Option[Int] =
        var n = size/osPageSize
        if size%osPageSize!=0 then n++
        db.freelist.allocate(txid,n)
    
    // construct a block object according size, then set its id
    private[platdb] def makeBlock(id:Int,size:Int):Block = 
        var bk = db.blockpool.get(size)
        bk.setid(id)
        blocks.addOne((id,bk))
        bk 
