package platdb

import java.nio.ByteBuffer
import scala.collection.mutable.{ArrayBuffer,Map}
import scala.util.control.Breaks._

private[platdb] object Meta:
    val elementSize = 52
    def size:Int = BlockHeader.size+elementSize
    def apply(data:Array[Byte]):Option[Meta] =
        if data.length < BlockHeader.size+ elementSize then
            throw new Exception("illegal meta data")
        BlockHeader(data.slice(0,BlockHeader.size)) match
            case None => throw new Exception("parse block header data failed")
            case Some(hd) =>
                var bk = new Block(data.length)
                bk.header = hd
                bk.write(0,data)
                read(bk)
    // convert block data to meta.
    def read(bk:Block):Option[Meta] =
        if bk.header.flag!= metaType then 
            return None 
        bk.getBytes() match
            case None => None 
            case Some(data) => 
                var meta = new Meta(bk.id)
                if data.length != elementSize then
                    return None 
                val arr = for i <- 0 to 5 yield
                    var n:Long = (data(8*i) & 0xff) << 56 | (data(8*i+1) & 0xff) << 48 | (data(8*i+2) & 0xff) << 40 | (data(8*i+3) & 0xff) << 32
                    n = n | (data(8*i+4) & 0xff) << 24 | (data(8*i+5) & 0xff) << 16 | (data(8*i+6) & 0xff) << 8 | (data(8*i+7) & 0xff)
                    n
                val sz =  (data(48) & 0xff) << 24 | (data(49) & 0xff) << 16 | (data(50) & 0xff) << 8 | (data(51) & 0xff)
                meta.pageId = arr(0)
                meta.freelistId = arr(1)
                meta.txid = arr(2)
                meta.root = new bucketValue(arr(3),arr(4),arr(5))
                meta.pageSize = sz
                Some(meta)

// database meta info.
private[platdb] class Meta(val id:Long) extends Persistence:
    var pageSize:Int = 0
    var flag:Byte = metaType
    var freelistId:Long = -1
    var pageId:Long = -1
    var txid:Long = -1
    var root:bucketValue = null

    override def clone:Meta =
        var m = new Meta(id)
        m.pageSize = pageSize
        m.flag = flag
        m.freelistId = freelistId
        m.pageId = pageId
        m.txid = txid
        m.root = new bucketValue(root.root,root.count,root.sequence)
        m
    def size():Int = Meta.size
    def writeTo(bk:Block):Int =
        bk.header.pgid = id 
        bk.header.flag = flag
        bk.header.overflow = 0
        bk.header.count = 1
        bk.header.size = size()
        bk.append(bk.header.getBytes())
        bk.append(getBytes())
        size()
    def getBytes():Array[Byte] =
        var buf:ByteBuffer = ByteBuffer.allocate(Meta.elementSize)
        buf.putLong(pageId)
        buf.putLong(freelistId)
        buf.putLong(txid)
        buf.putLong(root.root)
        buf.putLong(root.count)
        buf.putLong(root.sequence)
        buf.putInt(pageSize)
        buf.array()

// record freelist basic info, for example, count | type
private[platdb] case class FreelistHeader(count:Long,ftype:Byte)
// record a file free page fragement.
private[platdb] case class FreeFragment(start:Long,end:Long,length:Int):
    override def toString(): String = s"($start,$end,$length)"
// record a file pages free claim about a version txid.
private[platdb] case class FreeClaim(txid:Long, ids:ArrayBuffer[FreeFragment])

// freelist implement.
private[platdb] class Freelist(var header:BlockHeader) extends Persistence:
    // idle pages list, that can be allocated for read-write transaction.
    var idle:ArrayBuffer[FreeFragment] = new ArrayBuffer[FreeFragment]()
    //
    var unleashing:ArrayBuffer[FreeClaim] = new ArrayBuffer[FreeClaim]()
    // trace allocated pages for tx.
    var allocated:Map[Long,ArrayBuffer[FreeFragment]] = Map[Long,ArrayBuffer[FreeFragment]]() 

    override def toString(): String =
        val list = for f <- idle yield f.toString()
        list.mkString(",")
    
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
    def unleash(start:Long,end:Long):Unit =
        if start > end then return None
        unleashing.sortWith((c1:FreeClaim,c2:FreeClaim) => c1.txid < c2.txid)
        var i = 0
        var j = unleashing.length-1
        while i<unleashing.length && unleashing(i).txid<start do 
            i+=1
        if i >= unleashing.length then
            return None
        while j>=0 && unleashing(j).txid>end do
            j-=1
        if j < 0 then
            return None
        for k <- i to j do
            idle++=unleashing(k).ids
            allocated.remove(unleashing(k).txid)
        unleashing.remove(i,j-i+1)
        idle = reform(false)

    /**
      * Free up (tail+1) consecutive page spaces starting with startid.
      * Some pages may be released after a write transaction commited, 
      * and these pages will add a record 'txid: pageids' to the freelist's 
      * pending list to indicate that these pages with version number txid need to be freed.
      *
      * @param txid
      * @param startid
      * @param tail
      */
    def free(txid:Long,start:Long,tail:Int):Unit = // TODO Int-->Long
        val end = start+tail
        for f <- idle do
            if !(end < f.start || f.end < start) then
                throw new Exception(s"release repeatedly,tx $txid try to release [$start,$end], but already released [${f.start},${f.end}]")
        var idx = -1
        for i <- 0 until unleashing.length do
            // check
            for f <- unleashing(i).ids do
                if !(end < f.start || f.end < start) then
                    throw new Exception(s"release repeatedly,tx $txid try to release [$start,$end], but tx ${unleashing(i).txid} already released [${f.start},${f.end}]")
            if unleashing(i).txid == txid then
                idx = i
        if idx >=0 then 
            unleashing(idx).ids+= FreeFragment(start,end,tail+1)
        else
            var fc = FreeClaim(txid, new ArrayBuffer[FreeFragment]())
            fc.ids += FreeFragment(start,end,tail+1)
            unleashing += fc

    /**
      * Allocate contiguous space of size n*osPageSize and return the id of the first page, 
      * if there is no space in the current idle list that meets the conditions, it will return -1
      * (in this case, you need to allocate space from the end of the file and grow the file)
      *
      * @param txid
      * @param n
      * @return pgid
      */
    def allocate(txid:Long,n:Int):Long = 
        var idx = -1
        breakable(
            for i <- 0 until idle.length do
                if idle(i).length >= n then
                    idx = i
                    break()
        )
        if idx < 0 then
            return idx

        var ff = idle(idx)
        var fr:FreeFragment = null
        if n == ff.length then
            fr = ff
            idle.remove(idx)
        else
            val start = ff.start + n
            idle(idx) = FreeFragment(start,ff.end,(ff.end-start+1).toInt)
            fr = FreeFragment(ff.start,start-1,n)
        
        if !allocated.contains(txid) then
            allocated(txid) = new ArrayBuffer[FreeFragment]()
        else
            // check
            for f <- allocated(txid) do
                if !( fr.end < f.start || f.end < fr.start) then
                    throw new Exception(s"allocate repeatedly,tx $txid try to allocate [${fr.start},${fr.end}], but tx $txid already allocated [${f.start},${f.end}]")
        
        allocated(txid) += fr
        ff.start 

    /**
      * rollback pages release/allocate operations about txid.
      *
      * @param txid
      */
    def rollback(txid:Long):Unit =
        // return the assigned pages to the idle list.
        allocated.remove(txid) match
            case None => None
            case Some(fs) =>
                idle++=fs
                idle = reform(false)
        
        // retract the statement about the release of pages.
        var idx = -1
        breakable(
            for i <- 0 until unleashing.length do
                if unleashing(i).txid == txid then
                    idx = i 
                    break()
        )
        if idx >=0 then
            unleashing.remove(idx)
    
    /**
      * merge FreeFragment array elements and sort it by pgid.
      *
      * @param arr
      * @return
      */
    private def reform(merge:Boolean):ArrayBuffer[FreeFragment] =
        var arr = new Array[FreeFragment](idle.length)
        idle.copyToArray(arr)
        if merge then
            for fc <- unleashing do
                arr++=fc.ids 
        arr = arr.sortWith((f1:FreeFragment,f2:FreeFragment) => f1.start < f2.start)
        // merge
        var mg = new ArrayBuffer[FreeFragment]()
        var i = 0
        while i<arr.length do
            var j = i+1
            while j<arr.length && arr(j-1).end == arr(j).start-1 do
                j+=1
            if j!=i+1 then
                mg+=FreeFragment(arr(i).start,arr(j-1).end,(arr(j-1).end-arr(i).start+1).toInt)
            else
                mg+=arr(i)
            i = j 
        mg = mg.sortWith((f1:FreeFragment,f2:FreeFragment) => f1.length < f2.length || (f1.length == f2.length && f1.start < f2.start))
        mg
    /**
      * 
      *
      * @return
      */
    def size():Int = 
        var sz = BlockHeader.size + Freelist.headerSize + idle.length*Freelist.elementSize
        for fc <- unleashing do
            sz+= fc.ids.length*Freelist.elementSize
        sz 
    def writeTo(bk:Block):Int =
        val ids = reform(true)
        val sz = BlockHeader.size+Freelist.headerSize+ids.length*Freelist.elementSize

        bk.header.flag = freelistType
        bk.header.count = 1
        bk.header.size = sz
        bk.header.overflow =(sz+DB.pageSize)/DB.pageSize - 1
        
        bk.append(bk.header.getBytes())
        bk.append(Freelist.marshalHeader(FreelistHeader(ids.length,Freelist.listType)))
        for ff <- ids do
            bk.append(Freelist.marshalElement(ff))
        size()

private[platdb] object Freelist:
    val headerSize = 9
    val elementSize = 12
    val listType:Byte = 0
    val hashType:Byte = 1
    
    def apply(bk:Block):Option[Freelist] = 
        if bk.header.flag!=freelistType then
            throw new Exception(s"block type is not freelist ${bk.header.flag}") 
        bk.getBytes() match
            case None => None 
            case Some(data) => 
                var freelist = new Freelist(bk.header)
                if data.length < headerSize then
                    throw new Exception(s"illegal freelist header data length ${data.length}") 
                unmarshalHeader(data.slice(0,headerSize)) match
                    case None => throw new Exception("illegal freelist header data")
                    case Some(hd) =>
                        if data.length != headerSize+(hd.count*elementSize) then
                            throw new Exception(s"illegal freelist data length ${data.length},except ${headerSize+(hd.count*elementSize)}") 
                        
                        var idx = headerSize+elementSize
                        while idx <= data.length do
                            unmarshalElement(data.slice(idx-elementSize,idx)) match
                                case None => throw new Exception("illegal freelist element data")
                                case Some(ff) =>
                                    freelist.idle+=ff 
                                    idx+=elementSize
                        return Some(freelist)
        None 
    // parse freelist from raw bytes data.
    def getFromBytes(data:Array[Byte]):Option[Freelist] =
        if data.length < BlockHeader.size + headerSize then
            throw new Exception(s"illegal freelist data length ${data.length}")
        BlockHeader(data.slice(0,BlockHeader.size)) match
            case None => throw new Exception("parse freelist block header data failed")
            case Some(hd) =>
                var bk = new Block(data.length)
                bk.header = hd
                bk.write(0,data)
                apply(bk)
    //
    def unmarshalHeader(data:Array[Byte]):Option[FreelistHeader] =
        if data.length != headerSize then
            throw new Exception("illegal freelist header data")
        var count:Long = (data(0) & 0xff) << 56 | (data(1) & 0xff) << 48 | (data(2) & 0xff) << 40 | (data(3) & 0xff) << 32
        count = count | (data(4) & 0xff) << 24 | (data(5) & 0xff) << 16 | (data(6) & 0xff) << 8 | (data(7) & 0xff)
        Some(FreelistHeader(count,data(headerSize-1)))
    // 
    def marshalHeader(hd:FreelistHeader):Array[Byte] = 
        var c = hd.count
        var arr = new Array[Byte](headerSize)
        for i <- 0 to 7 do
            arr(7-i) = (c & 0xff).toByte
            c = c >> 8
        arr(headerSize-1) = hd.ftype
        arr
    //
    def unmarshalElement(data:Array[Byte]):Option[FreeFragment] =
        if data.length != elementSize then
            throw new Exception("illegal freelist element data")
        var s:Long = (data(0) & 0xff) << 56 | (data(1) & 0xff) << 48 | (data(2) & 0xff) << 40 | (data(3) & 0xff) << 32
        s = s | (data(4) & 0xff) << 24 | (data(5) & 0xff) << 16 | (data(6) & 0xff) << 8 | (data(7) & 0xff) 
        val l = (data(8) & 0xff) << 24 | (data(9) & 0xff) << 16 | (data(10) & 0xff) << 8 | (data(11) & 0xff)
        Some(FreeFragment(s,s+l-1,l))
    //
    def marshalElement(ff:FreeFragment):Array[Byte] = 
        var s1 = (ff.start>>32).toInt
        var s2 = ff.start
        var l = ff.length
        var arr = new Array[Byte](elementSize)
        for i <- 0 to 3 do
            arr(3-i) = (s1 & 0xff).toByte
            arr(7-i) = (s2 & 0xff).toByte
            arr(11-i) = (l & 0xff).toByte
            s1 = s1 >> 8
            s2 = s2 >> 8
            l = l >> 8
        arr
