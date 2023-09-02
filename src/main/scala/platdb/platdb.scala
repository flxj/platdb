package platdb

import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
import scala.util.{Try,Failure,Success}
import java.util.concurrent.locks.ReentrantLock

// global options
object PlatDB:
    @main def main(args: String*) =
        println("hello,platdb")

//
case class Options(val timeout:Int,val bufSize:Int,val readonly:Boolean)

//
class DB(val path:String,val ops:Options):
    private[platdb] var fileManager:FileManager = null
    private[platdb] var freelist:Freelist = null
    private[platdb] var blockBuffer:BlockBuffer = null
    private[platdb] var meta:Meta = null
    private var rTx:ArrayBuffer[Tx] = null
    private var rwTx:Option[Tx] = None 
    private var openFlag:Boolean = false
    private var rwLock:ReentrantReadWriteLock = null
    private var metaLock:ReentrantLock = null

    def name:String = path
    def isReadonly:Boolean = ops.readonly
    def isClosed:Boolean = !openFlag
    def open():Try[Boolean] =
        try
            if openFlag then
                return Success(true)

            // 尝试打开文件,如果超时则抛出一个异常
            fileManager = new FileManager(path,ops.readonly)
            fileManager.open(ops.timeout)
            blockBuffer = new BlockBuffer(ops.bufSize,fileManager)

            // TODO: 如果是第一次open db, 如何初始化？！
            if fileManager.size ==0 then
                init()
          
            // read meta 
            meta = loadMeta(0)
            metaLock = new ReentrantLock()
          
            rTx = new ArrayBuffer[Tx]()
            if !ops.readonly then
                // read freelist
                freelist = loadFreelist(meta.freelistId)
                rwLock = new ReentrantReadWriteLock()
          
            openFlag = true
            Success(true)
        catch
            case ex:Exception => Failure(ex)
            case er:Error => Failure(er)
    // 初始化数据文件
    private def init():Unit =
        // 1.创建空的meta并写入文件

        // 2.创建空的freelist并写入文件

        // 3.创建空的root bucket并写入文件

        None
    // 可能返回异常
    private def loadMeta(id:Int):Meta =
      val data = fileManager.readAt(id,meta.size())
      Meta.readFromBytes(data) match
        case None => throw new Exception(s"not found meta data from page ${id}")
        case Some(m) => return m

    // 可能返回异常
    private def loadFreelist(id:Int):Freelist =
      val hd = fileManager.readAt(id,blockHeaderSize)
      Block.unmarshalHeader(hd) match
        case None => throw new Exception(s"not found freelist header from page ${id}")
        case Some(h) => 
          val data = fileManager.readAt(id,h.size)
          val bk = new Block(0,data.length)
          bk.header = h
          Freelist.read(bk) match
            case None => throw new Exception(s"not found freelist data from page ${id}")
            case Some(fl) => return fl
    
    // 关闭数据库,调用该函数会阻塞直到所有已经打开的事务被提交
    def close(): Try[Boolean] =
        try 
            rwLock.writeLock().lock()
            metaLock.lock()
            if !isClosed then
                openFlag = false
                fileManager.close()
                fileManager = null
                freelist = null
            Success(true)
        catch
            case e:Exception => Failure(e)
        finally
            metaLock.unlock()
            rwLock.writeLock().unlock()
    //
    def sync():Unit = None

    // 打开一个事务
    def begin(writable:Boolean):Try[Transaction] =
        if writable then
            beginRWTx()
        else
            beginRTx()
        
    // 执行一个读写事务
    def update(op:(Tx)=>Unit):Try[Boolean] =
        beginRWTx() match
            case Failure(e) => Failure(e)
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
                            case _ => None
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
                            case _ => None
                        Failure(e)
                finally
                    tx.rollbackTx()
    // 创建一个数据库快照
    def snapshot(path:String):Try[Int] = Failure(new Exception("not implement snapshot now"))

    // 打开一个读写事务
    private def beginRWTx():Try[Tx] =
        if ops.readonly then
            return Failure(new Exception("readonly mode,cannot create read write transaction"))
        try 
            rwLock.writeLock().lock()
            metaLock.lock()
            if isClosed then
                throw new Exception("db closed")
            var tx = new Tx(false)
            tx.init(this)
            rwTx = Some(tx)
            free()
            Success(tx)
        catch
            case e:Exception => Failure(e)
        finally
            metaLock.unlock()
    // 打开一个只读事务
    private def beginRTx():Try[Tx] =
        try 
          metaLock.lock()
          if isClosed then
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
      rTx.sortWith((t1:Tx,t2:Tx) => t1.id < t2.id)
      var minid:Int = Int.MaxValue
      if rTx.length > 0 then
        minid = rTx(0).id
      
      if minid >0 then
        freelist.unleash(0,minid-1)
      
      for tx <- rTx do
        freelist.unleash(minid,tx.id-1)
        minid = tx.id+1
      freelist.unleash(minid,Int.MaxValue)
    // TODO
    private[platdb] def grow(sz:Int):Try[Boolean] = Failure(new Exception("not implement grow now"))
    //
    private[platdb] def removeTx(txid:Int):Unit =
      var idx = -1
      breakable(
        for i <- 0 until rTx.length do
            if txid == rTx(i).id then
                idx = i
                break()
      )
      if idx >=0 then
        rTx.remove(idx,1)
      else
        rwTx match
          case None => None
          case Some(tx) => 
            if tx.id == txid then
              rwTx = None
        None
    private[platdb] def removeRTx():Unit =
        rwTx = None
        rwLock.writeLock().unlock()


