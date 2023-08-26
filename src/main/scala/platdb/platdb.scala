package platdb

import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
import scala.util.{Try,Failure,Success}
import java.util.concurrent.locks.Lock


object PlatDB:
    @main def main(args: String*) =
        println("hello,platdb")

case class Options(val timeout:Int,val bufSize:Int,val readonly:Boolean)

class DB(val path:String,val ops:Options):
    private[platdb] var fileManager:FileManager = null
    private[platdb] var freelist:FreeList = null
    private[platdb] var blockBuffer:BlockBuffer = null
    private[platdb] var meta:Meta = null
    private var closeFlag:Boolean = true
    private var rTx:ArrayBuffer[Tx]
    private var rwTx:Option[Tx] = None
    private var rwLock:ReentrantReadWriteLock
    private val metaLock:Lock

    def name:String = path
    def isReadonly:Boolean = ops.readonly
    def open():Try[Boolean] =
      try
          if !closeFlag then
            return Success(true)
          // 尝试打开文件,如果超时则抛出一个异常
          fileManager = new FileManager(path,ops.readonly)
          fileManager.open(ops.timeout)

          // TODO: 如果是第一次open db, 如何初始化？！

          // 读取meta 
          meta = loadMeta(0)
          
          if ops.readonly then
            return Success(true)

          // 读取freelist 
          freelist = loadFreelist(meta.freelistId)
          
          blockBuffer = new BlockBuffer(ops.bufSize,fileManager)
          rTx = new ArrayBuffer[Tx]()
          metaLock = new Lock()
          rwLock = new ReentrantReadWriteLock()
          closeFlag = false
          Try(true) 
      catch
          case ex:Exception => Failure(ex)
          case er:Error => Failure(er)
    
    // 可能返回异常
    private def loadMeta(id:Int):Meta =
      val data = fileManager.readAt(id,meta.size)
      Meta.readFromBytes(data) match
        case None => throw new Exception(s"read meta data from page ${id} failed")
        case Some(m) => return m

    // 可能返回异常
    private def loadFreelist(id:Int):Freelist
    
    // 关闭数据库,调用该函数会阻塞直到所有已经打开的事务被提交
    def close(): Try[Boolean] =
      try 
        rwLock.writeLock().lock()
        metaLock.lock()
        if !closed then
          closeFlag = true
          fileManager.close()
          fileManager = null
          freelist = null
        Success(true)
      catch
        case e:Exception => Failure(e)
      finally
        metaLock.unlock()
        rwLock.writeLock().unlock()

    def closed:Boolean = closeFlag
    def sync():Unit

    // 打开一个事务
    def begin(writable:Boolean):Try[Tx] =
        if writable then
          beginRWTx()
        else
          beginRTx()
    // 执行一个读写事务
    def update(op:(Tx)=>Unit):Try[Boolean] =
      beginRWTx() match
          case Failure(exception) => Failure(exception)
          case Success(tx) =>
            try 
              // set current tx as a system transaction, that not allow rollback it manually.
	            tx.sysCommit = true
              op(tx)
              tx.sysCommit = false
              tx.commit()
            catch
              case e:Exception =>
                tx.rollback() match
                  case Failure(ex) => _ // TODO: if has log,record it
                Failure(e)
            finally
              tx.rollbackTx()

    // 执行一个只读事务
    def view(op:(Tx)=>Unit):Try[Boolean] =
      beginRTx() match
          case Failure(exception) => Failure(exception)
          case Success(tx) =>
            try 
	            tx.sysCommit = true
              op(tx)
              tx.sysCommit = false
              tx.rollback()
            catch
              case e:Exception =>
                tx.rollback() match
                  case Failure(ex) => _
                Failure(e)
            finally
              tx.rollbackTx()
    // 创建一个数据库快照
    def snapshot(path:String):Try[Int]

    // 打开一个读写事务
    private def beginRWTx():Try[Tx] =
      if ops.readonly then
        throw new Exception("readonly mode,cannot create read write transaction")
      try 
        rwLock.writeLock().lock()
        metaLock.lock()
        if closed then
            throw new Exception("db closed")
        var tx = new Tx(false)
        tx.init(this)
        rwTx = tx
        free()
        Success(rwTx)
      catch
        case e:Exception => Failure(e)
      finally
        metaLock.unlock()
    // 打开一个只读事务
    private def beginRTx():Try[Tx] =
        try 
          metaLock.lock()
          if closed then
            throw new Exception("db closed")
          var tx = new Tx(true)
          tx.init(this)
          rTx.addOne(tx)
          Success(tx)
        catch
          case e:Exception => Failure(e)
        finally
          metaLock.unlock()
    // 释放已经关闭的读事务持有的pages
    private def free():Unit =
      rTx.sortWith((t1:Tx,t2:Tx) => t1.txid < t2.txid)
      var minid:Long = Long.MaxValue
      if rTx.length > 0 then
        minid = rTx(0).txid
      
      if minid >0 then
        freelist.unleash(0,minid-1)
      
      for tx <- rTx do
        freelist.unleash(minid,tx.txid-1)
        minid = tx.txid+1
      freelist.unleash(minid,Long.MaxValue)
    //
    private[platdb] def grow(sz:Int):Boolean
    //
    private[platdb] def removeTx(txid:Int):Unit =
      var idx = -1
      breakable(
        for (i,tx) <- rTx do
            if txid == tx.id then
                idx = i
                break()
      )
      if idx >=0 then
        rTx.remove(idx,1)
      else
        rwTx match
          case Some(tx) => 
            if tx.id == txid then
              rTx = None

