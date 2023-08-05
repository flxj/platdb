package platdb
/*
txid可以表示db的一个版本(版本是保护整个db的)，读事务不会改变db的txid(它只能持有一份该读事务创建时候db最新的已经提交的版本), 写事务成功提交后会将db当前的txid+1, 并且由于写事务对db的增删改操作都是新建dirty page,所以不会影响之前的版本。

写事务提交后可能会释放一些page, 这会在freelist的pending中添加一条记录 'txid: pageids' 表示txid版本的这些pageids需要释放

下一个写事务在开始执行之前会尝试调用freelist释放pending中的page, 只要pending中待释放的版本小于当前打开的只读事务持有的最小版本，那末就可以释放这些pending元素 （表示当前肯定已经没有只读事务在持有这些待释放pages了）

同时当前已经打开的只读事务持有的版本可能跨度较大， 那末对于两个相邻版本之间的版本， 如果已经没有事务在持有它，那末也是可以释放的


# 只读事务：

[1] db.Begin()
先获取meta page互斥lock, 再获取mmap读锁
创建一个Tx实例，调用Tx.init(db) 初始化该实例
-- 拷贝meta page
-- 拷贝root bucket
将该tx对象加入到db的事务列表(只读事务列表)， boltdb支持并发读，并且同时至多有一个写事务，写事务之间串行执行
释放meta page互斥锁
更新db的事务状态信息(事务个数，当前打开的事务个数等统计信息)

[2] 执行事务操作
-- 只读事务一般会
[3] 调用t.Rollback()结束事务
对于只读事务因为没有对数据进行过修改，所以其rollback操作就是调用tx.db.removeTx(tx),将事务实例从db的事务列表中移除
-- 释放mmap读锁
-- 获取meta page互斥锁
-- 更新db的事务列表
-- 释放meta page互斥锁
-- 更新db的事务状态信息

# 读写事务：
[1] db.Begin()
先获取一个读写锁的写锁(目的是保证同一时间至多只能有一个写事务存在)
获取meta page互斥锁
创建一个Tx实例，调用Tx.init(db) 初始化该实例
-- 拷贝meta page
-- 拷贝root bucket
-- 初始化tx.pages缓存，将tx.meta.txid加1
调用db.freePages()释放已经关闭的只读事务关联的所有pages (找到当前只读事务列表中txid最小的txid值，然后将freelist.pending中所有小于该txid的page都释放)
-- 获取db的事务列表中txid最小的事务，记其id为minid，如果存在则调用db.freelist.release(minid - 1)
-- 遍历db事务列表，依次调用db.freelist.releaseRange(minid, t.meta.txid-1)
-- 调用db.freelist.releaseRange(minid, txid(0xFFFFFFFFFFFFFFFF))
释放meta page互斥锁
[2] 执行事务操作
-- 读写事务一般会
[3] 如果执行事务没出错则tx.Commit()提交事务的修改
-- tx.root.rebalance() 节点合并操作，该操作涉及freelist释放pgid （被释放的pgid会进入freelist.pending中而不会立即用于再次分配，这是因为可能有其它读事务正在依赖该page,需要等系统中没有任何读事务持有该page后才能将该pgid用于分配；如果贸然释放就可能导致读事务读到新的写事务尚未提交的修改）
-- tx.root.spill() 将节点拆分成osPagesize大小，该操作涉及从freelist处分配pgid
-- tx.meta.root.root = tx.root.root 将meta page的root bucket设置为最新的，释放原来旧的root
-- tx.db.freelist.free(tx.meta.txid, tx.db.page(tx.meta.freelist)) 释放旧的freelist
-- tx.commitFreelist() 提交新的freelist
-- 如果当前db下一个要分配的pgid值,tx.meta.pgid> 旧meta.pgid， 则需要扩大数据库tx.db.grow(int(tx.meta.pgid+1) *tx.db.pageSize)
-- tx.write() 将修改写入文件: 将该写事务创建的dirty pages写入文件
-- tx.writeMeta() 将meta page写入文件
-- tx.close()
[4] 如果执行事务出错则调用tx.Rollback()
-- tx.db.freelist.rollback(tx.meta.txid)
-- tx.close()

*/
class Tx(val readonly:Boolean):
    var db:DB = _ 
    var meta:Meta = _ 
    var root:Bucket = _ 
    var blocks:Map[Int,Block] = _  // 缓存的dirty block (bucket的rebalance和spill操作产生的， 一个事务可能涉及多个bucket, 所有的dirty block都放这里)

    def id:Int = 0 
    def blockId:Int = meta.blockId
    def writable:Boolean = !readonly
    def commit():Unit
    def rollback():Unit
    def close():Unit
    def bucket(name:String):Option[Bucket] = None 
    def createBucket(name:String):(Option[Bucket],String) = (None,None)
    def createBucketIfNotExists(name:String):(Option[Bucket],String) = (None,None)
    def deleteBucket(name:String):Unit

    def writeFreelist():Unit // 将freelist写入文件
    def writeBlock():Unit  // 将该事务的dirty blocks写入文件

    def free(id:Int):Unit // 调用db.freelist.free(txid,start,tail),释放缓存的block
    def allocate(size:Int):Option[Int] // 调用db.freelist.allocate(),分配pgid
    def block(id:Int):Option[Block] // 根据id查询缓存的block
    def makeBlock(id:Int,size:Int):Block //  调用db.filemanager.allocate(size) 或者db.blockpool.allocate(size)方法得到一个空间合适的block, block会被缓存套blocks


    
// 返回Unit的方法都可能抛出异常