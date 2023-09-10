package platdb

import java.nio.ByteBuffer
import java.io.File
import java.io.IOError
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.RandomAccessFile
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.util.{Try,Success,Failure}
import java.nio.channels.FileLock
import java.nio.channels.FileChannel
import java.util.Timer
import java.util.Date
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.util.control.Breaks._
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayDeque

private[platdb] trait Persistence:
    /** return object bytes size. */
    def size():Int
    /**
      * write object content to block.
      *
      * @param block
      * @return size of writed into
      */
    def writeTo(block:Block):Int

val osPageSize:Int = 64 // 4096

// block/node type
val metaType:Int = 1
val branchType:Int = 2
val leafType:Int = 3
val freelistType:Int = 4

// node element type
val bucketType:Int = 5

//
private[platdb] class BlockHeader(var pgid:Int,var flag:Int,var count:Int,var overflow:Int,var size:Int)

// uid 可能会用于blockBuffer pool管理block用
private[platdb] class Block(val cap:Int):
    var header:BlockHeader = null
    private var data:ArrayBuffer[Byte] = new ArrayBuffer[Byte](cap) // TODO:use Array[Byte] ?
    private var idx:Int = 0 // index of the data tail.
     
    def id:Int = header.pgid
    // the actual length of data that has been used.
    def size:Int = idx
    // total capacity.
    def capacity:Int = data.length
    // block type.
    def btype:Int = header.flag 
    def setid(id:Int):Unit = header.pgid = id 
    def reset():Unit = idx = 0
    def write(offset:Int,d:Array[Byte]):Unit =
        if offset<0 || d.length == 0 then 
            return None
        if offset+d.length > capacity then 
            data++=new Array[Byte](offset+d.length-capacity)
        for i <- 0 until d.length do  // TODO: use copy
            data(offset+i) = d(i)
        if d.length+offset > idx then
            idx = d.length+offset
    // write data to tail.
    def append(d:Array[Byte]):Unit = write(idx,d)
    // all data.
    def all:Array[Byte] = data.toArray
    // user data.
    def getBytes():Option[Array[Byte]] = 
        if header.size >= Block.headerSize then
            return Some(data.slice(Block.headerSize,header.size).toArray)
        else if idx>= Block.headerSize then
            return Some(data.slice(Block.headerSize,idx).toArray)
        None
    // all data except header.
    def tail:Option[Array[Byte]] = 
        if capacity > Block.headerSize then
            Some(data.takeRight(Block.headerSize).toArray)
        else
            None 

private[platdb] object Block:
    val headerSize = 20
    def marshalHeader(pg:BlockHeader):Array[Byte] =
        var buf:ByteBuffer = ByteBuffer.allocate(headerSize)
        buf.putInt(pg.pgid)
        buf.putInt(pg.flag)
        buf.putInt(pg.count)
        buf.putInt(pg.overflow)
        buf.putInt(pg.size)
        buf.array()
    def unmarshalHeader(bs:Array[Byte]):Option[BlockHeader] =
        if bs.length != headerSize then 
            None 
        else
            var arr = new Array[Int](headerSize/4)
            var i = 0
            while i<headerSize/4 do 
                var n = 0
                for j <- 0 to 3 do // TODO: not use loop here.
                    n = n << 8
                    n = n | (bs(4*i+j) & 0xff)
                arr(i) = n
                i = i+1
            Some(new BlockHeader(arr(0),arr(1),arr(2),arr(3),arr(4)))

// blockBuffer 不用考虑数据的一致性，只要调用就尝试返回即可
private[platdb] class BlockBuffer(val maxsize:Int,var fm:FileManager):
    //private var id = new AtomicInteger(1)

    // Save some useless blocks that have been kicked out of the cache queue to speed up the creation of block structures.
    val poolsize:Int = 16
    var idleLock:ReentrantReadWriteLock = new ReentrantReadWriteLock()
    var idle:ArrayBuffer[Block] = new ArrayBuffer[Block]() // TODO: 加入一些统计，对get频率高的size多缓存几个
    
    // Records the blocks that are currently in use and maintains a reference count of them.
    var lock:ReentrantReadWriteLock = new ReentrantReadWriteLock()
    var count:Int = 0
    var blocks:Map[Int,Block] = Map[Int,Block]()
    var pinned:Map[Int,Int] = Map[Int,Int]() 
    // LRU
    var link:ArrayDeque[Int] = new ArrayDeque[Int]()

    private def full:Boolean = count>=maxsize
    private def drop(bk:Block):Unit=
        if idle.length <= poolsize then
            try 
                idleLock.writeLock().lock()
                idle+=bk
            finally
                idleLock.writeLock().unlock()
    def get(size:Int):Block =
        try 
            idleLock.writeLock().lock()
            var idx:Int = -1
            breakable(
                for i <- 0 until idle.length do
                    if idle(i).capacity >= size then
                        idx = i
                        break()
            )
            var bk:Block = null
            if idx >= 0 then
                bk = idle.remove(idx)
            else
                bk = new Block(size)
            bk 
        finally
            idleLock.writeLock().unlock()

    def revert(id:Int):Unit =
        try 
            lock.writeLock().lock()

            val n = pinned.getOrElse(id,0)
            if n>1 then
                pinned(id) = n-1
            else
                pinned.remove(id)
        finally
            lock.writeLock().unlock()
    // 
    def read(pgid:Int):Try[Block] = 
        try 
            lock.writeLock().lock()
            // query cache.
            var bk:Block = null
            var cached:Boolean = false
            blocks.get(pgid) match
                case Some(bk) => 
                    val n = pinned.getOrElse(pgid,0)
                    pinned(pgid)=n+1
                    cached = true
                case None => 
                    fm.read(pgid) match
                        case (None,_) => 
                            return Failure(new Exception(s"not found block header for pgid ${pgid}"))
                        case (Some(hd),None) => 
                            return Failure(new Exception(s"not found block data for pgid ${pgid}"))
                        case (Some(hd),Some(data)) =>
                            // get a block from idle.
                            bk = get(hd.size)
                            bk.header = hd 
                            bk.write(0,data)
        
            var idx:Int = -1 // will remove the element from link.
            var ignore:Boolean = false
            if !cached then
                // cache not full, so cache the block directly.
                if !full then
                     blocks(bk.id) = bk
                     count+=1
                else
                    // cache already full, so try to select a element to eliminate.
                    breakable(
                        for i <- Range(link.length-1,-1,-1) do
                            if !pinned.contains(link(i)) then
                                idx = i
                                break()
                    )
                    if idx < 0 then // cache is busy
                        ignore = true
            else
                // the block has cached, so just move it to head of link.
                breakable(
                    for i <- Range(0,link.length,1) do
                        if link(i) == bk.id then
                            idx = i
                            break()
                )

            if idx >= 0 then
                val id = link(idx)
                if !cached then
                    // remove the selected block from blocks, and throw it to idle list.
                    blocks.remove(id) match
                        case None => None
                        case Some(blk) => drop(blk)
                    blocks+=(bk.id,bk)
                link.remove(id)
            
            if !ignore then
                link.prepend(bk.id)
            
            Success(bk)      
        catch
            case e:Exception => return Failure(e)
        finally
            lock.writeLock().unlock()

    def write(bk:Block):Try[Boolean] = 
        var writed:Boolean = false 
        try 
            fm.write(bk)
            lock.writeLock().lock()
            writed = true

            if !full then
                if !blocks.contains(bk.id) then
                    blocks(bk.id) = bk
                    count+=1
            else
                None // TODO 
            Success(writed)
        catch
            case e:Exception => return Failure(e)
        finally
            if writed then
                lock.writeLock().unlock()
        
    // TODO: sync将所有脏block写入文件？
    def sync():Unit = None

// 
private[platdb] class FileManager(val path:String,val readonly:Boolean):
    var opend:Boolean = false
    var file:File = null
    var writer:RandomAccessFile = null // writer
    var channel:FileChannel = null
    var lock:FileLock = null
    //
    def size:Long = 
        if !opend then 
            throw new Exception(s"file ${path} not open")
        file.length()
    // open and lock.
    def open(timeout:Int):Unit =
        if opend then return None 
        file = new File(path)
        if !file.exists() then
            file.createNewFile()
        var mode:String = "rw"
        if readonly then
            mode = "r"
        writer = new RandomAccessFile(file,mode)
        channel = writer.getChannel()
        
        val start = new Date()
        while !opend do
            lock = channel.tryLock(0L,Long.MaxValue,readonly)
            if lock != null then
                opend = true
            else
                val now = new Date()
                val d = now.getTime() - start.getTime()
                if d/1000 > timeout then
                    throw new Exception(s"open database timeout:${timeout}s")
    // close and unlock.
    def close():Unit =
        if !opend then
            return None
        channel.close()
        writer.close()
        lock.release()
        opend = false
    // grow file.
    def grow(sz:Long):Unit = 
        if sz <= size then return None
        channel.truncate(sz)
    
    def readAt(id:Int,size:Int):Array[Byte] =
        if !opend then
            throw new Exception("db file closed")
        if id<0 then 
            throw new Exception(s"illegal page id ${id}")
        var reader:RandomAccessFile = null
        try 
            reader = new RandomAccessFile(file,"r")
            reader.seek(id*osPageSize)
            var data = new Array[Byte](size)
            if reader.read(data,0,size)!= size then
              throw new Exception("read size is unexpected")
            data
        catch
            case e:Exception => throw e
        finally
            if reader!=null then
                reader.close()
    //
    def read(bid:Int):(Option[BlockHeader],Option[Array[Byte]]) = 
        if !opend then
            throw new Exception("db file closed")
        if bid<0 then 
            throw new Exception(s"illegal block id ${bid}")
        val offset = bid*osPageSize
        var reader:RandomAccessFile = null
        try
            reader = new RandomAccessFile(file,"r")
            // 1. use pgid to seek block header location in file
            reader.seek(offset)
            // 2. read the block header content
            var hd = new Array[Byte](Block.headerSize)
            if reader.read(hd,0,Block.headerSize)!= Block.headerSize then
              throw new Exception(s"read block header ${bid} error")
            
            Block.unmarshalHeader(hd) match
                case None => throw new Exception(s"parse block header ${bid} error")
                case Some(bhd) =>
                    // 3. get overflow value, then read continue pages of number overflow 
                    val sz = (bhd.overflow+1)*osPageSize
                    var data = new Array[Byte](sz)
                    if reader.read(data,0,sz) != sz then
                        throw new Exception("read block data size unexpected")
                    (Some(bhd),Some(data)) 
        finally
            if reader!=null then
                reader.close()
    //  
    def write(bk:Block):Boolean = 
        if !opend then
            throw new Exception("db file closed")
        if bk.size == 0 then 
            return true
        if bk.id <=1 && bk.btype != metaType then // TODO remove this check to tx
            throw new Exception(s"block type error ${bk.btype}")
        writer.seek(bk.id*osPageSize)
        writer.write(bk.all)
        true
        
