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

/**
  * 
  *
  * @param timeout
  * @param bufSize
  * @param readonly
  * @param fillPercent
  */
case class Options(val timeout:Int,val bufSize:Int,val readonly:Boolean,val fillPercent:Double)

/**
  * 
  */
object DB:
    val maxKeySize = 32768 // 32k
    val maxValueSize = (1 << 31) - 2
    val defaultBufSize = 128
    val defaultPageSize = 4096 // 4k
    val defaultTimeout = 2000 // 2s
    val defaultFillPercent = 0.5
    val minFillPercent = 0.1
    val maxFillPercent = 1.0
    private[platdb] var pageSize = defaultPageSize
    private[platdb] var fillPercent = defaultFillPercent
    private[platdb] val meta0Page = 0
    private[platdb] val meta1Page = 1

    val exceptionTxClosed = new Exception("transaction is closed")
    val exceptionNotAllowOp = new Exception("readonly transaction not allow current operation")
    val exceptionKeyIsNull = new Exception("param key is null")
    val exceptionKeyTooLarge = new Exception(s"key is too large,limit $maxKeySize")
    val exceptionValueTooLarge = new Exception(s"value is too large,limit $maxValueSize")
    val exceptionValueNotFound = new Exception("value not found")
    val exceptionDBClosed = new Exception("db is closed")
    val exceptionNotAllowRWTx = new Exception("readonly mode,cannot create read write transaction")
    val exceptionNotAllowCommitSysTx = new Exception("not allow to commit system transaction manually")
    val exceptionNotAllowRollbackSysTx = new Exception("not allow to rollback system transaction manually")
    val exceptionNotAllowCommitRTx = new Exception("cannot commit read-only tx")
    
    def isNotExists(e:Exception):Boolean = false // TODO
    def isAlreadyExists(e:Exception):Boolean = false

/**
  * 
  */
given defaultOptions:Options = Options(DB.defaultTimeout,DB.defaultBufSize,false,DB.defaultFillPercent)

/**
  * DB is
  *
  * 
  */
class DB(val path:String)(using ops:Options):
    private[platdb] var fileManager:FileManager = null
    private[platdb] var freelist:Freelist = null
    private[platdb] var blockBuffer:BlockBuffer = null
    private[platdb] var meta:Meta = null
    private var rTx:ArrayBuffer[Tx] = new ArrayBuffer[Tx]()
    private var rwTx:Option[Tx] = None 
    private var openFlag:Boolean = false
    private var rwLock:ReentrantReadWriteLock = new ReentrantReadWriteLock()
    private var metaLock:ReentrantLock = new ReentrantLock()

    def name:String = path
    def readonly:Boolean = ops.readonly
    def closed:Boolean = !openFlag
    def pageSize:Int = DB.pageSize
    def fillPercent:Double = DB.fillPercent
    /**
      * open database.
      *
      * @return
      */
    def open():Try[Boolean] =
        try
            if openFlag then
                return Success(true)

            // try to open db file, if get lock timeout,then return an exception.
            fileManager = new FileManager(path,ops.readonly)
            fileManager.open(ops.timeout)
            // init block buffer.
            blockBuffer = new BlockBuffer(ops.bufSize,fileManager)

            // if is the first time opened db, need init db file.
            if fileManager.size ==0 then
                init()
          
            // read meta info.
            meta = loadMeta(0)
            DB.pageSize = meta.pageSize
          
            rTx = new ArrayBuffer[Tx]()
            if !ops.readonly then
                // read freelist info.
                freelist = loadFreelist(meta.freelistId)
            
            if ops.fillPercent < DB.minFillPercent then
                DB.fillPercent = DB.minFillPercent
            else if ops.fillPercent > DB.maxFillPercent then
                DB.fillPercent = DB.maxFillPercent
            else 
                DB.fillPercent = ops.fillPercent
                
            openFlag = true
            Success(openFlag)
        catch
            case ex:Exception => Failure(ex)
            case er:Error => Failure(er)
    /**
      * init db file.
      */
    private def init():Unit =
        // 1.create two null meta object and write to file.
        for i<- 0 to 1 do
            var m = new Meta(i)
            m.flags = metaType
            m.pageSize = DB.defaultPageSize
            m.txid = 0
            m.freelistId = 2
            m.pageId = 4
            m.root = new bucketValue(3,0,0)

            var bk = blockBuffer.get(DB.defaultPageSize)
            m.writeTo(bk)
            blockBuffer.write(bk) match
                case Success(_) => None
                case Failure(e) => throw e 

        // 2.create a null freelist and write to file.
        var fl = new Freelist(new BlockHeader(2,freelistType,0,0,0))
        var fbk = blockBuffer.get(DB.defaultPageSize)
        fbk.setid(2)
        val n = fl.writeTo(fbk) 
        blockBuffer.write(fbk) match
            case Success(_) => None
            case Failure(e) => throw e 

        // 3.craete null root bucket.
        var root = new Node(new BlockHeader(3,leafType,0,0,0))
        var rbk = blockBuffer.get(DB.defaultPageSize)
        rbk.setid(3)
        root.writeTo(rbk)
        blockBuffer.write(rbk) match
            case Success(_) => None
            case Failure(e) => throw e
        None
    
    /**
      * read meta info from page.
      *
      * @param id
      * @return
      */
    private def loadMeta(id:Int):Meta =
        val data = fileManager.readAt(id,Meta.size)
        Meta.readFromBytes(data) match
            case None => throw new Exception(s"not found meta data from page ${id}")
            case Some(m) => return m

    /**
      * read freelist info from page.
      *
      * @param id
      * @return
      */
    private def loadFreelist(id:Int):Freelist =
        val hd = fileManager.readAt(id,Block.headerSize)
        Block.unmarshalHeader(hd) match
            case None => throw new Exception(s"not found freelist header from page ${id}")
            case Some(h) => 
                val data = fileManager.readAt(id,h.size)
                var bk = new Block(data.length)
                bk.header = h
                bk.append(data)
                Freelist(bk) match
                    case None => throw new Exception(s"not found freelist data from page ${id}")
                    case Some(fl) => fl
    /**
      * close database. this method will block until all opended transaction be commited or rollbacked.
      *
      * @return
      */
    def close(): Try[Boolean] =
        try 
            rwLock.writeLock().lock()
            metaLock.lock()
            if !closed then
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

    /**
      * open a transaction.
      *
      * @param writable
      * @return
      */
    def begin(writable:Boolean):Try[Transaction] =
        if writable then
            beginRWTx()
        else
            beginRTx()
    /**
      * 
      *
      * @param writable
      * @param timeout
      * @return
      */
    def tryBegin(writable:Boolean,timeout:Int):Try[Transaction] = Failure(new Exception("not implement now"))
    /**
      * get a element from bucket.
      *
      * @param bucket
      * @param key
      * @return
      */
    def get(bucket:String,key:String):Try[(String,String)] = 
        var value:String = ""
        beginRTx() match
            case Failure(e) => Failure(e)
            case Success(tx) =>
                try 
                    tx.sysCommit = true
                    tx.openBucket(bucket) match
                        case Failure(e) => throw e
                        case Success(bk) => value = bk(key)
                    tx.sysCommit = false
                    tx.commit() match
                        case Success(_) => None
                        case Failure(e) => throw e
                    Success((key,value))
                catch
                    case e:Exception =>
                        tx.rollback() match
                            case _ => None
                        Failure(e)
                finally
                    tx.rollbackTx()
    /**
      * get key-value pairs from a bucket.
      *
      * @param bucket
      * @param keys
      * @return
      */
    def get(bucket:String,keys:String*):Try[Seq[(String,String)]] =
        var res = List[(String,String)]()
        beginRTx() match
            case Failure(e) => Failure(e)
            case Success(tx) =>
                try 
                    tx.sysCommit = true
                    tx.openBucket(bucket) match
                        case Failure(e) => throw e
                        case Success(bk) =>
                            for key <- keys do
                                bk.get(key) match
                                    case Failure(e) => throw e 
                                    case Success(value) => 
                                        res :+= (key,value)
                    tx.sysCommit = false
                    tx.commit() match
                        case Success(_) => None
                        case Failure(e) => throw e
                    Success(res)
                catch
                    case e:Exception =>
                        tx.rollback() match
                            case _ => None
                        Failure(e)
                finally
                    tx.rollbackTx()
    /**
      * 
      *
      * @param bucket
      * @param key
      * @param value
      * @return
      */
    def put(bucket:String,key:String,value:String):Try[Boolean] = 
        beginRWTx() match
            case Failure(e) => Failure(e)
            case Success(tx) =>
                try 
                    tx.sysCommit = true
                    tx.openBucket(bucket) match
                        case Failure(e) => throw e
                        case Success(bk) => bk+(key,value)
                    tx.sysCommit = false
                    tx.commit()
                catch
                    case e:Exception =>
                        tx.rollback() match
                            case _ => None
                        Failure(e)
                finally
                    tx.rollbackTx()
    /**
      * 
      *
      * @param bucket
      * @param elems
      * @return
      */
    def put(bucket:String,elems:Seq[(String,String)]):Try[Boolean] = 
        beginRWTx() match
            case Failure(e) => Failure(e)
            case Success(tx) =>
                try 
                    tx.sysCommit = true
                    tx.openBucket(bucket) match
                        case Failure(e) => throw e
                        case Success(bk) => bk+(elems)
                    tx.sysCommit = false
                    tx.commit()
                catch
                    case e:Exception =>
                        tx.rollback() match
                            case _ => None
                        Failure(e)
                finally
                    tx.rollbackTx()
    /**
      * try to exec a read-write transaction.
      *
      * @param op
      * @return
      */
    def update(op:(Transaction)=>Unit):Try[Boolean] =
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
    /**
      * try to exec a read-only transaction.
      *
      * @param op
      * @return
      */
    def view(op:(Transaction)=>Unit):Try[Boolean] =
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
    /**
      * 
      *
      * @param path
      * @return
      */
    def backup(path:String):Try[Int] = Failure(new Exception("not implement backup now"))

    /**
      * open a read-write transaction.
      *
      * @return
      */
    private def beginRWTx():Try[Tx] =
        if ops.readonly then
            return Failure(DB.exceptionNotAllowRWTx)
        try 
            rwLock.writeLock().lock()
            metaLock.lock()
            if closed then
                throw DB.exceptionDBClosed
            var tx = new Tx(false)
            tx.init(this)
            rwTx = Some(tx)
            free()
            Success(tx)
        catch
            case e:Exception => Failure(e)
        finally
            metaLock.unlock()
    /**
      * open a read-only transcation.
      *
      * @return
      */
    private def beginRTx():Try[Tx] =
        try 
            metaLock.lock()
            if closed then
                throw DB.exceptionDBClosed
            var tx = new Tx(true)
            tx.init(this)
            rTx.addOne(tx)
            Success(tx)
        catch
            case e:Exception => Failure(e)
        finally
            metaLock.unlock()
    
    /**
      * release all pages,that has no more need by current opened transactions.
      */
    private def free():Unit =
        rTx = rTx.sortWith((t1:Tx,t2:Tx) => t1.id < t2.id)
        var minid:Int = Int.MaxValue
        if rTx.length > 0 then
            minid = rTx(0).id
      
        if minid >0 then
            freelist.unleash(0,minid-1)
      
        for tx <- rTx do
            freelist.unleash(minid,tx.id-1)
            minid = tx.id+1
        freelist.unleash(minid,Int.MaxValue)
    /**
      * read-write transaction will update the meta info at the end of commit process.
      *
      * @param m
      */
    private[platdb] def updateMeta(m:Meta):Unit =
        try 
            metaLock.lock()
            meta = m
        finally
            metaLock.unlock()
    /**
      * grow the db file to size.
      *
      * @param sz
      * @return
      */
    private[platdb] def growTo(sz:Long):Try[Boolean] = 
        try
            fileManager.grow(sz)
            Success(true)
        catch
            case e:Exception => return Failure(new Exception(s"grow db failed:${e.getMessage()}"))
    /**
      * remove the transcation object from db tx cache.
      *
      * @param txid
      */
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
                        removeRTx()
    /**
      * remove the read-write transaction.
      */
    private[platdb] def removeRTx():Unit =
        rwTx = None
        rwLock.writeLock().unlock()


