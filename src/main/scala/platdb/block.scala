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

trait Persistence:
    def size():Int
    def block():Block 
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
class BlockHeader(var id:Int,var flag:Int,var count:Int,var overflow:Int,var size:Int)

// uid 可能会用于blockBuffer pool管理block用
private[platdb] class Block(val uid:Int,val sz:Int): // header对象应该是可变的，因为可能需要为其分配id
    var header:BlockHeader = _
    var data:ArrayBuffer[Byte] = new ArrayBuffer[Byte](sz) // header 和实际data都放到data中
    private var idx:Int = 0 // idx总是指向下一个写入位置
     
    def id:Int = header.id
    def uid:Int = uid 
    // 返回实际已经使用的容量
    def size:Int = idx
    // 容量：data字段的物理长度
    def capacity:Int = data.length
    // block类型
    def btype:Int = header.flag 
    def setid(id:Int):Unit = header.id = id 
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
//
protected class BlockBuffer(val size:Int):
    private var id = new AtomicInteger(1)
    def idle:Map[Int,Block] = _
    def get(size:Int):Block =
        val bid = id.getAndIncrement()
        var bk = new Block(bid,size)
        return bk 
    def revert(uid:Int):Uint = None

// 
protected class FileManager(val path:String, val fmType:String,var pool:BlockBuffer):
    var f:File = _
    var opend:Boolean = _
    def size:Option[Long] = 
        if !opend then None 
        f.length()

    def open():Boolean
        if opend then true 
        try 
            f = new File(path)
            if !f.exists() then
                f.createNewFile()
            open = true 
        catch 
            case e:Exception => false 
            case _ => true 

    def grow(size:Int):Boolean = 
        if !opend || size<=0 then return false 
        var w:FileOutputStream = _ 
        try 
            val arr = new Array[Byte](size)
            val idx = f.length().intValue()

            w = new FileOutputStream(f)
            w.write(arr,idx,arr.length)
            true 
        catch 
            case e:Exception => return false  
        finally 
            w.close()
    //
    def read(bid:Int):Option[Block] = 
        if !opend || bid<0 then 
            return None 
        val offset = bid*osPageSize
        var reader:RandomAccessFile
        try
            reader = new RandomAccessFile(f,"rw")
            // 1. use pgid to seek block header location in file
            reader.seek(offset)
            // 2. read the block header content
            var hd = new Array[Byte](blockHeaderSize)
            if reader.read(hd,0,blockHeaderSize)!= blockHeaderSize then
              throw new Exception("read block header error")
            
            Block.unmarshalHeader(hd) match
                case None => throw new Exception("parse block header error")
                case Some(bhd) =>
                    // 3. get overflow value, then read continue pages of number overflow 
                    val sz = (bhd.overflow+1)*osPageSize
                    var data = new Array[Byte](sz)
                    if reader.read(data,0,sz) != sz then
                        throw new Exception("read block data error")
                    // get a block from pool
                    var bk = pool.get(sz)
                    bk.header = bhd 
                    bk.write(0,data)
                    Some(bk) 
        catch
            case e:Exception => None
        finally
            reader.close()
    //   
    def write(bk:Block):(Boolean,Int) = 
        if !opend || bk.size == 0 then 
            return (false,0)
        var w:RandomAccessFile
        try 
            if bk.id <=1 && bk.btype!=metaType then 
                throw new Exception(s"block type error ${bk.btype}")
            w = new RandomAccessFile(f,"rw")
            w.seek(bk.id*osPageSize)
            w.write(bk.data)
            (true,bk.size)
        catch 
            case e:Exception => (false,0)
        finally
            w.close()


    