package platdb

import java.nio.ByteBuffer
import java.io.File
import java.io.IOError
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.RandomAccessFile

trait Persistence:
    def size():Int
    def block():Block 
    def writeTo(block:Block):Int 

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
case class BlockHeader(id:Int,flag:Int,count:Int,overflow:Int,size:Int)  extends Serializable

protected class Block:
    var header:BlockHeader // 该对象应该是可变的，因为可能需要为其分配id
    var data:Array[Byte] // index和data区合并为data
    var idx:Int 
    val uid:Int // uid 可能会用于blockpool管理block用
    
    def bid:Int = header.id
    def size:Int = 0 // 返回实际已经使用的容量 data[0:idx]
    def btype:Int = 0 // block类型： 叶子节点还是分支节点
    def setid(id:Int):Unit
    def append(d:Array[Byte]):Uint // 追加data
    def capacity:Int // 容量：data字段的物理长度
    def data:Array[Byte] = data.slice(0,idx)

protected object Block:
    def MarshalHeader(pg:BlockHeader):Array[Byte] =
        var buf:ByteBuffer = ByteBuffer.allocate(blockHeaderSize)
        buf.putInt(pg.id)
        buf.putInt(pg.flag)
        buf.putInt(pg.count)
        buf.putInt(pg.overflow)
        buf.putInt(pg.size)
        buf.array()
    
    def UnmarshalHeader(bs:Array[Byte]):Option[BlockHeader] =
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


//  对于bucket类型的header该 

    
/* how to load a logic page from file?
按照osPageSize将文件划分成若干物理段，若干物理上连续的page组成一个逻辑block, 而一个block对应于内存中的一个B+树node

1. use pgid to seek pageheader location in file
2. read the pageheader
3. get overflow value, then read continue pages of number overflow 
4. read size bytes and  unmashal it to Node
*/

// 
protected class FileManager(val path:String, val fmType:String):
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

    def grow(size:Int):Option[Int] = 
        if !opend || size<=0 then None 
        var w:FileOutputStream
        try 
            val arr = new Array[Byte](size)
            val idx = f.length().intValue()

            w = new FileOutputStream(f)
            w.write(arr,idx,arr.length)
            Some(size)
        catch 
            case e:Exception => None 
            case _ => Some(0)
        finally 
            w.close()

    def read(bid:Int):Option[Block] = 
        if !opend || bid<0 then None 
        val offset = pgid*osPageSize
        var reader:RandomAccessFile
        try
            reader = new RandomAccessFile(f,"rw")
            reader.seek(offset)
            // 1. get header
            var hd = new Array[Byte](blockHeaderSize)
            if reader.read(hd,0,blockHeaderSize)!= blockHeaderSize then
              throw new Exception("read block header error")
            var Some(pg) = Block.Unmarshal(hd)

            // 2. get data
            reader.seek(offset+blockHeaderSize)
            var data = new Array[Byte](pg.size)
            if reader.read(data,0,pg.size) != pg.size then
                throw new Exception("read block data error")
            Some(new Block(pg,data)) // TODO: 从缓存中拿Block
        catch 
            case e:Exception => None 
            case _ => None
        finally
            reader.close()
        
    def write(bk:Block):Option[Int] = 
        if !opend || bk.size == 0 then None 
        var w:RandomAccessFile
        try 
            val phd = Block.MarshalHeader(bk.header)
            if phd.id <=1 && phd.btype!=metaType then 
                throw new Exception("block type error")
            val d = phd ++ bk.data

            w = new RandomAccessFile(f,"rw")
            w.seek(hd.id*osPageSize)
            w.write(d)
            Some(d.length)
        catch 
            case e:Exception => None 
            case _ => Some(0)
        finally 
            w.close()


    