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

trait Persistence:
    def size():Int
    def writeTo(block:Block):Int // 返回写入的字节数

/* node
+------------------+-----------------------------------------+
|     header       |                data                     |
+------------------+-----------------+------------+----------+
| block header     |     block index |      block data       |
+------------------+-----------------+------------+----------+
*/
val maxKeySize:Int = 0
val maxValueSize:Int = 0

val osPageSize:Int = 64 // 4096
val blockHeaderSize:Int = 20

val metaType:Int = 1
val branchType:Int = 2
val leafType:Int = 3
val freelistType:Int = 4
val bucketType:Int = 5

@SerialVersionUID(100L)
class BlockHeader(var pgid:Int,var flag:Int,var count:Int,var overflow:Int,var size:Int)

// uid 可能会用于blockBuffer pool管理block用
private[platdb] class Block(val uid:Int,val sz:Int): // header对象应该是可变的，因为可能需要为其分配id
    var header:BlockHeader = _
    var data:ArrayBuffer[Byte] = new ArrayBuffer[Byte](sz) // header 和实际data都放到data中
    private var idx:Int = 0 // idx总是指向下一个写入位置
     
    def id:Int = header.pgid
    def uid:Int = uid 
    // 返回实际已经使用的容量
    def size:Int = idx
    // 容量：data字段的物理长度
    def capacity:Int = data.length
    // block类型
    def btype:Int = header.flag 
    def setid(id:Int):Unit = header.pgid = id 
    def reset():Unit = idx = 0
    def write(offset:Int,d:Array[Byte]):Unit =
        if offset<0 || d.length == 0 then 
            return None
        if offset+d.length >= capacity then 
            data++=new ArrayBuffer[Byte](offset+d.length-capacity)
        if d.copyToArray(data,offset)!= d.length then
            return None
        if d.length+offset > idx then
            idx = d.length+offset
    // 追加data
    def append(d:Array[Byte]):Uint = write(idx,d)
    // 所有数据
    def all:ArrayBuffer[Byte] = data.slice(0,idx)
    // 返回除了header外的数据
    def tail:Option[ArrayBuffer[Byte]] = 
        if size>blockHeaderSize then
            Some(data.slice(blockHeaderSize,size))
        else
            None 

protected object Block:
    def marshalHeader(pg:BlockHeader):Array[Byte] =
        var buf:ByteBuffer = ByteBuffer.allocate(blockHeaderSize)
        buf.putInt(pg.id)
        buf.putInt(pg.flag)
        buf.putInt(pg.count)
        buf.putInt(pg.overflow)
        buf.putInt(pg.size)
        buf.array()
    def unmarshalHeader(bs:Array[Byte]):Option[BlockHeader] =
        if bs.length != blockHeaderSize then 
            None 
        else
            var arr = new Array[Int](blockHeaderSize/4)
            var i = 0
            while i<blockHeaderSize/4 do 
                var n = 0
                for j <- 0 to 3 do
                    n = n << 8
                    n = n | (bs(4*i+j) & 0xff)
                arr(i) = n
                i = i+1
            Some(new BlockHeader(arr(0),arr(1),arr(2),arr(3),arr(4)))

// blockBuffer 不用考虑数据的一致性，只要调用就尝试返回即可
protected class BlockBuffer(val size:Int,var fm:FileManager):
    private var id = new AtomicInteger(1)
    var idle:Map[Int,Block] = _ // 保存一些被从缓存队列中踢出来的无用block,便于快速创建block结构
    var pool:ArrayBuffer[Block] = _  // block缓存
    var pinned:Map[Int,Int] = _ // 记录缓存队列中配pinned的block, key为block pgid, value为计数器

    // TODO: 获取一个满足空间大小的空block
    def get(size:Int):Block =
        val bid = id.getAndIncrement()
        var bk = new Block(bid,size)
        return bk 
    // TODO: 从缓存中read的block,需要归还
    def revert(uid:Int):Uint = None 
    // 
    def read(pgid:Int):Try[Block] = 
        // TODO: 先查看缓存有没有
        try 
            fm.read(pgid) match
                case (None,_) => 
                    return Failure(new Exception(s"not found block header for pgid ${pgid}"))
                case (Some(hd),None) => 
                    return Failure(new Exception(s"not found block data for pgid ${pgid}"))
                case (Some(hd),Some(data)) =>
                    // get a block from idle
                    var bk = get(hd.size)
                    bk.header = hd 
                    bk.write(0,data)
                    return Success(bk)
                    // TODO: 是否缓存读到的block
        catch
            case e:Exception => return Failure(e)

    def write(bk:Block):Try[Boolean] = 
        // TODO: 是否缓存该block？ 是否延迟写入文件？
        try 
            fm.write(bk)
            Success(true)
        catch
            case e:Exception => return Failure(e)
        
    // TODO: sync将所有脏block写入文件？
    def sync():Unit

// 
protected class FileManager(val path:String,val readonly:Boolean):
    var opend:Boolean
    var file:File = null
    var writer:RandomAccessFile = null // writer
    var channel:FileChannel = null
    var lock:FileLock = null
    //
    def size:Option[Long] = 
        if !opend then None 
        file.length()
    // open and lock 
    def open(timeout:Int):Unit
        if opend then return None 
        file = new File(path)
        if !file.exists() then
            file.createNewFile()
        //
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
    // close and unlock
    def close():Unit =
        if !opend then
            return None
        channel.close()
        writer.close()
        lock.release()
        opend = false

    def grow(size:Int):Unit = 
        if !opend then
            throw new Exception("db file closed")
        if size<0 then 
            return None // TODO shrink the file
        if size == 0 then
            return None

        var w:FileOutputStream = _ 
        try 
            val arr = new Array[Byte](size)
            val idx = file.length().intValue()

            w = new FileOutputStream(file) // TODO:
            w.write(arr,idx,arr.length)
        finally 
            w.close()
    
    def readAt(id:Int,size:Int):Array[Byte] =
        if !opend then
            throw new Exception("db file closed")
        if bid<0 then 
            throw new Exception(s"illegal page id ${bid}")
        var reader:RandomAccessFile
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
            reader.close()
    //
    def read(bid:Int):(Option[BlockHeader],Option[Array[Byte]]) = 
        if !opend then
            throw new Exception("db file closed")
        if bid<0 then 
            throw new Exception(s"illegal block id ${bid}")
        val offset = bid*osPageSize
        var reader:RandomAccessFile
        try
            reader = new RandomAccessFile(file,"r")
            // 1. use pgid to seek block header location in file
            reader.seek(offset)
            // 2. read the block header content
            var hd = new Array[Byte](blockHeaderSize)
            if reader.read(hd,0,blockHeaderSize)!= blockHeaderSize then
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
            reader.close()
    //  
    def write(bk:Block):Boolean = 
        if !opend then
            throw new Exception("db file closed")
        if bk.size == 0 then 
            return true
        if bk.pgid <=1 && bk.btype != metaType then // TODO remove this check to tx
            throw new Exception(s"block type error ${bk.btype}")
        writer.seek(bk.pgid*osPageSize)
        writer.write(bk.data)
        true
        
