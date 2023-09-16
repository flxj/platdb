package platdb

import java.nio.ByteBuffer
import scala.collection.mutable.{ArrayBuffer,Map}
import scala.util.control.Breaks._
import platdb.Meta.elementSize

private[platdb] object Meta:
    val elementSize = 32
    def readFromBytes(data:Array[Byte]):Option[Meta] =
        if data.length < Block.headerSize+ elementSize then
            throw new Exception("illegal meta data")
        Block.unmarshalHeader(data.slice(0,Block.headerSize)) match
            case None => throw new Exception("parse block header data failed")
            case Some(hd) =>
                var bk = new Block(data.length)
                bk.header = hd
                bk.write(0,data)
                return read(bk)
    //
    def read(bk:Block):Option[Meta] =
        if bk.header.flag!= metaType then 
            return None 
        bk.getBytes() match
            case None => None 
            case Some(data) => 
                var meta = new Meta(bk.id)
                if data.length!= elementSize then
                    return None 
                var arr = new Array[Int](6)
                var s:Long = 0
                for i <- 0  until 6 do
                    var n  = 0 
                    for j <- 0 to 3 do
                        n = n << 8
                        n = n | (data(4*i+j) & 0xff)
                    arr(i) = n
                for i <- 0 to 7 do 
                    s = s << 8
                    s = s | (data(24+i) & 0xff)
                meta.pageSize = arr(0)
                meta.freelistId = arr(1)
                meta.pageId = arr(2)
                meta.txid = arr(3)
                meta.root = new bucketValue(arr(4),arr(5),s)
                Some(meta)

    def marshal(meta:Meta):Array[Byte] =
        var buf:ByteBuffer = ByteBuffer.allocate(elementSize)
        buf.putInt(meta.pageSize)
        buf.putInt(meta.freelistId)
        buf.putInt(meta.pageId)
        buf.putInt(meta.txid)
        buf.putInt(meta.root.root)
        buf.putInt(meta.root.count)
        buf.putLong(meta.root.sequence)
        buf.array()

private[platdb] class Meta(val id:Int) extends Persistence:
    var pageSize:Int = 0
    var flags:Int = metaType
    var freelistId:Int = -1// 记录freelist block的pgid
    var pageId:Int = -1
    var txid:Int = -1
    var root:bucketValue = null

    def size():Int = Block.headerSize+ elementSize
    def writeTo(bk:Block):Int =
        bk.header.pgid = id 
        bk.header.flag = flags
        bk.header.overflow = 0
        bk.header.count = 1
        bk.header.size = size()
        bk.append(Block.marshalHeader(bk.header))
        bk.write(bk.size,Meta.marshal(this))
        bk.size
    override def clone:Meta =
        var m = new Meta(id)
        m.pageSize = pageSize
        m.flags = flags
        m.freelistId = freelistId
        m.pageId = pageId
        m.txid = txid
        m.root = new bucketValue(root.root,root.count,root.sequence)
        m

// record freelist basic info, for example, count | type
private[platdb] case class FreelistHeader(count:Int,ftype:Int)
// record a file free page fragement.
private[platdb] case class FreeFragment(start:Int,end:Int,length:Int)
// record a file pages free claim about a version txid.
private[platdb] case class FreeClaim(txid:Int, ids:ArrayBuffer[FreeFragment])

// freelist implement.
private[platdb] class Freelist(var header:BlockHeader) extends Persistence:
    var idle:ArrayBuffer[FreeFragment] = new ArrayBuffer[FreeFragment]()
    var unleashing:ArrayBuffer[FreeClaim] = new ArrayBuffer[FreeClaim]()
    // trace allocated pages for tx.
    var allocated:Map[Int,ArrayBuffer[FreeFragment]] = Map[Int,ArrayBuffer[FreeFragment]]() 

    def size():Int = 
        var sz = Block.headerSize + Freelist.headerSize + idle.length*Freelist.elementSize
        for fc <- unleashing do
            sz+= fc.ids.length*Freelist.elementSize
        sz 
    
    /**
      * Release page: move all pages about txid from pending queue to idle queue.
      * 
      * the next write transaction will try to call the Freelist Unleash method to free the pages in the pending before starting execution, 
      * and as long as the version to be released in the pending is less than the minimum version held by the currently open read-only transaction, 
      * then the pending elements can be released (indicating that there are definitely no read-only transactions holding the pending pages anymore)
      * at the same time, the version held by the currently open read-only transaction may span a large span, so for pages between two adjacent versions, 
      * if no transaction is already holding it, then it can also be released
      *
      * @param start
      * @param end
      */
    def unleash(start:Int,end:Int):Unit =
        if start > end then return None
        unleashing.sortWith((c1:FreeClaim,c2:FreeClaim) => c1.txid < c2.txid)
        var i = 0
        var j = unleashing.length-1
        while i<unleashing.length && unleashing(i).txid<start do 
            i+=1
        while j>0 && unleashing(j).txid>end do
            j-=1
        for k <- i to j do
            idle++=unleashing(k).ids
        unleashing = unleashing.slice(0,i) ++ unleashing.takeRight(j+1)
        idle = reform()

    /**
      * Free up (tail+1) consecutive page spaces starting with startid.
      * Some pages may be released after a write transaction commited, 
      * and these pages will add a record 'txid: pageids' to the freelist's pending list to indicate that these pages with version number txid need to be freed.
      *
      * @param txid
      * @param startid
      * @param tail
      */
    def free(txid:Int,startid:Int,tail:Int):Unit = 
        val ff = FreeFragment(startid,startid+tail,tail+1)
        var idx = -1
        breakable(
            for i <- 0 until unleashing.length do
                if unleashing(i).txid == txid then
                    idx = i 
                    break()
        )
        if idx >=0 then 
            unleashing(idx).ids+=ff 
        else
            var fc = FreeClaim(txid, new ArrayBuffer[FreeFragment]())
            fc.ids+=ff
            unleashing+=fc 

    /**
      * Allocate contiguous space of size n*osPageSize and return the id of the first page, 
      * if there is no space in the current idle list that meets the conditions, it will return -1
      * (in this case, you need to allocate space from the end of the file and grow the file)
      *
      * @param txid
      * @param n
      * @return pgid
      */
    def allocate(txid:Int,n:Int):Int = 
        var i = 0
        while i<idle.length && n>idle(i).length do
            i+=1
        if i>=idle.length then
            return -1
        val ff = idle(i)
        if n == ff.length then
            idle = idle.slice(0,i) ++ idle.takeRight(i+1)
            if !allocated.contains(txid) then
                allocated.addOne((txid,new ArrayBuffer[FreeFragment]()))
            allocated(txid) += ff 
        else
            val start = ff.start + n
            idle(i) = FreeFragment(start,ff.end,ff.end-start+1)
            if !allocated.contains(txid) then
                allocated.addOne((txid,new ArrayBuffer[FreeFragment]()))
            allocated(txid) +=  FreeFragment(ff.start,start-1,n)
        ff.start  

    // rollback pages release/allocate operations about txid.
    def rollback(txid:Int):Unit =
        // return the assigned pages to the idle list.
        allocated.remove(txid) match
            case None => None
            case Some(fs) =>
                idle++=fs
                idle = reform()
        
        // retract the statement about the release of pages.
        var idx = -1
        breakable(
            for i <- 0 until unleashing.length do
                if unleashing(i).txid == txid then
                    idx = i 
                    break()
        )
        if idx >=0 then
            unleashing = unleashing.slice(0,idx)++unleashing.takeRight(idx+1)

    private def reform():ArrayBuffer[FreeFragment] =
        var arr = new Array[FreeFragment](idle.length)
        idle.copyToArray(arr,0)
        arr.sortWith((f1:FreeFragment,f2:FreeFragment) => f1.start < f2.start)
        makeIdle(arr)

    // merge unleashing and idle list.
    private def merge():ArrayBuffer[FreeFragment] = 
        var arr = new Array[FreeFragment](idle.length)
        idle.copyToArray(arr,0)
        for fc <- unleashing do
            arr++=fc.ids 
        makeIdle(arr) 
    
    // merge FreeFragment array elements and sort it by pgid.
    private def makeIdle(arr:Array[FreeFragment]):ArrayBuffer[FreeFragment] =
        arr.sortWith((f1:FreeFragment,f2:FreeFragment) => f1.start < f2.start)
        // merge
        var mg = new ArrayBuffer[FreeFragment]()
        var i = 0
        while i<arr.length do
            var j = i+1
            while j<arr.length && arr(j-1).end == arr(j).start -1 do
                j+=1
            if j!=i+1 then
                mg+=FreeFragment(arr(i).start,arr(j).end,arr(j).end-arr(i).start+1)
            else
                mg+=arr(i)
            i = j+1 
        mg.sortWith((f1:FreeFragment,f2:FreeFragment) => f1.length < f2.length || (f1.length == f2.length && f1.start < f2.start))
        mg
    //
    def writeTo(bk:Block):Int =
        bk.header.flag = freelistType
        bk.header.count = 1
        bk.header.size = size()
        var ovf = (size()/DB.pageSize)-1
        if size()%DB.pageSize!=0 then 
            ovf+=1
        bk.header.overflow = ovf

        val ids = merge() 
        bk.append(Block.marshalHeader(bk.header))
        bk.append(Freelist.marshalHeader(new FreelistHeader(ids.length,Freelist.listType)))
        for ff <- ids do
            bk.append(Freelist.marshalElement(ff))
        size()

private[platdb] object Freelist:
    val headerSize = 8
    val elementSize = 8
    val listType = 0
    val hashType = 1
    
    def read(bk:Block):Option[Freelist] = 
        if bk.header.flag!=freelistType then 
            throw new Exception(s"block type is not freelist ${bk.header.flag}") 
        
        bk.getBytes() match
            case None => None 
            case Some(data) => 
                var freelist = new Freelist(bk.header)
                freelist.header = bk.header
                if data.length < Freelist.headerSize then
                    throw new Exception("illegal freelist header data") 
                unmarshalHeader(data.slice(0,Freelist.headerSize)) match
                    case None => throw new Exception("illegal freelist header data")
                    case Some(hd) =>
                        if data.length != Freelist.headerSize+(hd.count*Freelist.elementSize) then
                            throw new Exception("illegal freelist data") 
                        freelist.idle = new ArrayBuffer[FreeFragment]()
                        freelist.unleashing = new ArrayBuffer[FreeClaim]()
                        freelist.allocated = Map[Int,ArrayBuffer[FreeFragment]]()

                        var idx = Freelist.headerSize+Freelist.elementSize
                        while idx <= data.length do
                            unmarshalElement(data.slice(idx-Freelist.elementSize,idx)) match
                                case None => throw new Exception("illegal freelist element data")
                                case Some(ff) =>
                                    freelist.idle+=ff 
                        return Some(freelist)
        None 
    // parse freelist from raw bytes data.
    def readFromBytes(data:Array[Byte]):Option[Freelist] =
        if data.length < Block.headerSize + Freelist.headerSize then
            throw new Exception("illegal freelist data")
        Block.unmarshalHeader(data.slice(0,Block.headerSize)) match
            case None => throw new Exception("parse block header data failed")
            case Some(hd) =>
                var bk = new Block(data.length)
                bk.header = hd
                bk.write(0,data)
                return read(bk)
    //
    def unmarshalHeader(data:Array[Byte]):Option[FreelistHeader] =
        var count = 0
        var ftype = 0
        for i <- 0 to 3 do
            count = count << 8
            count = count | (data(i) & 0xff)
            ftype = ftype << 8
            ftype = ftype | (data(4+i) & 0xff)
        Some(FreelistHeader(count,ftype))

    def marshalHeader(hd:FreelistHeader):Array[Byte] = 
        /*
        var buf:ByteBuffer = ByteBuffer.allocate(Freelist.headerSize)
        buf.putInt(hd.count)
        buf.putInt(hd.ftype)
        buf.array()
        */
        var c = hd.count
        var t = hd.ftype
        var arr = new Array[Byte](headerSize)
        for i <- 0 to 3 do
            arr(3-i) = (c & 0xff).toByte
            arr(7-i) = (t & 0xff).toByte
            c = c >> 8
            t = t >> 8
        arr

    def unmarshalElement(data:Array[Byte]):Option[FreeFragment] =
        var s = 0
        var e = 0
        for i <- 0 to 3 do
            s = s << 8
            s = s | (data(i) & 0xff)
            e = e << 8
            e = e | (data(4+i) & 0xff)
        Some(FreeFragment(s,e,e-s+1))
    //
    def marshalElement(ff:FreeFragment):Array[Byte] = 
        /*
        var buf:ByteBuffer = ByteBuffer.allocate(elementSize)
        buf.putInt(ff.start)
        buf.putInt(ff.end)
        buf.array()
        */
        var s = ff.start
        var e = ff.end
        var arr = new Array[Byte](elementSize)
        for i <- 0 to 3 do
            arr(3-i) = (s & 0xff).toByte
            arr(7-i) = (e & 0xff).toByte
            s = s >> 8
            e = e >> 8
        arr


