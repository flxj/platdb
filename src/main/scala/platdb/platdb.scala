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
  * The DB object contains some default constant information as well as exception information.
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
  * Default DB configuration.
  */
given defaultOptions:Options = Options(DB.defaultTimeout,DB.defaultBufSize,false,DB.defaultFillPercent)

/**
  * DB represents a database object consisting of several buckets, each of which is a collection of key-value pairs (nested buckets are supported).
  * All operations performed by users on buckets are performed in transactions.
  * 
  */
class DB(val path:String)(using ops:Options):
    // fileManager provides operations for reading and writing DB files.
    private[platdb] var fileManager:FileManager = null
    // freeList provides the ability to manage db file pages.
    private[platdb] var freelist:Freelist = null
    // blockBuffer provides the function of caching the page of db files.
    private[platdb] var blockBuffer:BlockBuffer = null
    // the latest meta information for the current DB.
    private[platdb] var meta:Meta = null
    // records read-only, read-write transaction objects that are currently open.
    private var rTx:ArrayBuffer[Tx] = new ArrayBuffer[Tx]()
    private var rwTx:Option[Tx] = None 
    // Locks are used to control the execution of read and write transactions. 
    // PlatDB allows multiple read-only transactions and at most one read-write transaction to execute concurrently at the same time.
    private var rwLock:ReentrantReadWriteLock = new ReentrantReadWriteLock()
    // Used to protect meta information.
    private var metaLock:ReentrantLock = new ReentrantLock()
    private var openFlag:Boolean = false

    def name:String = path
    def readonly:Boolean = ops.readonly
    def closed:Boolean = !openFlag
    def pageSize:Int = DB.pageSize
    def fillPercent:Double = DB.fillPercent
    /**
      * The open method is used to open the DB, and if any exception is encountered, 
      * it will cause the opening to fail and return an error message. 
      * Currently, platdb does not support multiple processes opening the same DB at the same time, 
      * if a process wants to open a DB that is already held by another process,
      * the opening process will be blocked until the holder closes the DB or throws a timeout exception.
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
            m.flag = metaType
            m.pageSize = DB.defaultPageSize
            m.txid = 0
            m.freelistId = 2
            m.pageId = 4
            m.root = new btreeBucketValue(3,0,0)

            var bk = blockBuffer.get(DB.defaultPageSize)
            m.writeTo(bk)
            blockBuffer.write(bk) match
                case Success(_) => None
                case Failure(e) => throw e 

        // 2.create a null freelist and write to file.
        var fl = new Freelist(new BlockHeader(2L,freelistType,0,0,0))
        var fbk = blockBuffer.get(DB.defaultPageSize)
        fbk.setid(2)
        val n = fl.writeTo(fbk) 
        blockBuffer.write(fbk) match
            case Success(_) => None
            case Failure(e) => throw e 

        // 3.craete null root bucket.
        var root = new Node(new BlockHeader(3L,leafType,0,0,0))
        var rbk = blockBuffer.get(DB.defaultPageSize)
        rbk.setid(3)
        root.writeTo(rbk)
        blockBuffer.write(rbk) match
            case Success(_) => None
            case Failure(e) => throw e
    
    /**
      * read meta info from page.
      *
      * @param id
      * @return
      */
    private def loadMeta(id:Long):Meta =
        val data = fileManager.readAt(id,Meta.size)
        Meta(data) match
            case None => throw new Exception(s"not found meta data from page ${id}")
            case Some(m) => return m

    /**
      * read freelist info from page.
      *
      * @param id
      * @return
      */
    private def loadFreelist(id:Long):Freelist =
        val hd = fileManager.readAt(id,BlockHeader.size)
        BlockHeader(hd) match
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

    def sync():Try[Boolean] = Failure(new Exception("not implement now"))

    /**
      * Opens and returns a transactional object, or exception information if the opening fails.
      * If the writable parameter is true, the method creates a read-write transaction, and if false, it creates a read-only transaction. 
      * Note that due to the transaction concurrency control mechanism, if a user wants to open a read-write transaction, 
      * the method blocks until the previous read-write transaction is closed.
      *
      * @param writable
      * @return
      */
    def begin(writable:Boolean):Try[Transaction] = if writable then beginRWTx() else beginRTx()
    /**
      * This method is the same as the begin method, 
      * except that a timeout parameter is provided to prevent constant blocking when opening a read-write transaction.
      *
      * @param writable
      * @param timeout The unit is milliseconds
      * @return
      */
    def tryBegin(writable:Boolean,timeout:Int):Try[Transaction] = Failure(new Exception("not implement now"))
    /**
      * Retrieves the element of the specified key from the specified bucket.
      * returns a key-value pair if the element exists; If the element is a child bucket, an exception is returned.
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
      * Retrieves multiple elements of the specified key from the specified bucket. 
      * Returns a key-value pair sequence if the element exists; If the element contains subbuckets, an exception is returned.
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
                                    case Success(value) => res :+= (key,value)
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
      * Adds a key-value pair to the specified bucket, 
      * and its value is overwritten by the new value if the key already exists
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
      * Adds multiple key-value pairs to the specified bucket, 
      * and if some of the keys already exist, their values are overwritten by the new values.
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
      * Executes user functions in the context of a read-write transaction. 
      * If the function does not produce any exceptions, the transaction is automatically committed; Otherwise, automatic rollback.
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
      * Executes user functions in a read-only transaction context.
      * If the function does not produce any exceptions, the transaction is automatically committed; Otherwise, automatic rollback.
      * Performing a change database operation in a read-only transaction results in an exception.
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
      *  Hot backup of the current database.
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
        var minid:Long = Long.MaxValue
        if rTx.length > 0 then
            minid = rTx(0).id
      
        if minid >0 then
            freelist.unleash(0,minid-1)
      
        for tx <- rTx do
            freelist.unleash(minid,tx.id-1)
            minid = tx.id+1
        freelist.unleash(minid,Long.MaxValue)
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
    private[platdb] def removeTx(txid:Long):Unit =
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
