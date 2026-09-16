/*
   Copyright (C) 2023 flxj(https://github.com/flxj)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package platdb

import java.nio.ByteBuffer
import scala.collection.mutable.{ArrayBuffer,Map}
import scala.util.control.Breaks._
import scala.util.{Try,Success,Failure}
import scala.compiletime.ops.double

private[platdb] trait FreeManager extends Persistence:
    def pageId:Long 
    def overflow:Int 
    // Update the basic information of the freelist.
    def set(pgid:Long,overflow:Int):Unit
    // Allocate a continuous storage space of length
    // n * pagesize for transaction txid.
    def allocate(txid:Long,n:Int):Long
    /*
      The transaction declaration releases the continuous 
      (tailLen+1) page space starting from 'start', 
      and these pages will enter a queued state waiting for recycling
    */
    def reclaim(txid:Long,start:Long,tailLen:Int):Unit
    /*  
      Release the pages reclaimed from transactions with txid within 
      the closed interval [startTx, endTx] to the idle list.
    */
    def release(startTx:Long,endTx:Long):Unit  
    // Rollback the allocation or recycling operation of a transaction.
    def rollback(txid:Long):Unit

// record a file pages free claim about a version txid.
private case class ReleaseClaim(txid:Long, pages:ArrayBuffer[(Long,Long)])

// default freelist implement.
private[platdb] class FreeArray(var header:BlockHeader) extends FreeManager:
    var oldHeader:Option[BlockHeader] = None
    // idle pages list, that can be allocated for read-write transaction. sorted by pageid.
    var idle:ArrayBuffer[(Long,Long)] = new ArrayBuffer[(Long,Long)]()
    // Record space release requests for transactions.(sorted by txid)
    var pending:ArrayBuffer[ReleaseClaim] = new ArrayBuffer[ReleaseClaim]() 
    // trace allocated pages for tx.
    var allocated:Map[Long,ArrayBuffer[(Long,Long)]] = Map[Long,ArrayBuffer[(Long,Long)]]() 

    override def toString(): String =
        (for f <- idle yield f.toString()).mkString(",")
    def pageId: Long = header.pgid
    def overflow: Int = header.overflow
    def size():Int = 
        var sz = BlockHeader.size + FreeArray.headerSize + idle.length*FreeArray.elementSize
        for p <- pending do
            sz += p.pages.length*FreeArray.elementSize
        sz 
    
    def writeTo(bk:Block):Int =
        val pids = merge()
        val sz = BlockHeader.size+FreeArray.headerSize+pids.length*FreeArray.elementSize

        bk.header.flag = Block.typeFreelist
        bk.header.count = 1
        bk.header.size = sz
        bk.header.overflow =(sz+DB.pageSize)/DB.pageSize - 1
        
        bk.append(bk.header.getBytes())
        bk.append(FreeArray.headerToBytes(FreelistHeader(pids.length,FreeArray.listType)))
        for (s,e) <- pids do
            bk.append(FreeArray.elemToBytes(s,e))
        size()
    def set(pgid:Long,overflow:Int):Unit = 
        oldHeader = Some(header.clone)
        header.pgid = pgid
        header.overflow = overflow
    def release(startTx:Long,endTx:Long):Unit =
        if startTx > endTx then 
            None
        else
            val i = find((txid:Long) => txid >= startTx )
            if i >= pending.length then
                return None
            var j = find((txid:Long) => txid > endTx)
            for k <- i until j do 
                insert(pending(k).pages.toArray)
                allocated.remove(pending(k).txid)
            pending.remove(i,j-i)
    def reclaim(txid:Long,start:Long,tailLen:Int):Unit =
        val ed = start+tailLen
        // check idle list overleap
        for (l,r) <- idle do if !(ed < l || r < start) then
            throw new Exception(s"release repeatedly,tx $txid try to release [$start,$ed], but already released [${l},${r}]")
        // check pending list.
        var idx = -1
        for i <- 0 until pending.length do
            for (s,e) <- pending(i).pages do if !(ed < s || e < start) then
                throw new Exception(s"release repeatedly,tx $txid try to release [$start,$ed], but tx ${pending(i).txid} already released [${s},${e}]")
            if pending(i).txid == txid then
                idx = i
        if idx >= 0 then 
            pending(idx).pages += ((start,ed))
        else
            var fc = ReleaseClaim(txid, new ArrayBuffer[(Long,Long)]())
            fc.pages += ((start,ed))
            pend(fc)
    def allocate(txid:Long,n:Int):Long = 
        val (idx,equ) = scan(n)
        if idx < 0 then
            return idx
        // cut the pages segment.
        var pages = idle(idx)
        if equ then
            idle.remove(idx)
        else
            val start = pages(0) + n.toLong
            idle(idx) = (start,pages(1))
            pages = (pages(0),start-1)
        
        if !allocated.contains(txid) then
            allocated(txid) = new ArrayBuffer[(Long,Long)]()
        else
            for (s,e) <- allocated(txid) do
                if !(pages(1) < s || e < pages(0)) then
                    throw new Exception(s"allocate repeatedly,tx $txid try to allocate [${pages(0)},${pages(1)}], but tx $txid already allocated [${s},${e}]")
        allocated(txid) += pages
        pages(0)
    def rollback(txid:Long):Unit =
        // return the assigned pages to the idle list.
        allocated.remove(txid) match
            case None => None
            case Some(pages) => insert(pages.toArray)
        
        // retract the statement about the release of pages.
        val idx = find((id:Long) => id >= txid )
        if idx < pending.length && pending(idx).txid == txid then
            pending.remove(idx)

        // rollback header.
        oldHeader match
            case None => None
            case Some(hd) => header = hd
    // add pages to idle list.
    private def insert(start:Long,ed:Long):Unit = 
        val (i,ok) = search(start)
        if ok then
            idle(i) = ((start,ed))
        else
            if i >= idle.length then
                idle.append((start,ed))
            else
                idle.insert(i,(start,ed))
    private def insert(pages:Seq[(Long,Long)]):Unit = 
        for (s,e) <- pages do insert(s,e)
        idle = reduce(idle)
    // search pages segment in idle list
    private def search(id:Long):(Int,Boolean) = 
        var l = 0 
        var r = idle.length
        while l < r do 
            val m = (l+r)/2
            if idle(m)(0) == id then
                return (m,true)
            else if idle(m)(0) > id then
                r = m 
            else
                l = m+1
        (r,false)
    // scan idle list,finding pages space large or equels sz.
    private def scan(sz:Int):(Int,Boolean) = 
        var idx = -1
        var ok = false
        breakable(
            for (pg,i) <- idle.zipWithIndex do 
                if (pg(1)-pg(0)+1) >= sz.toLong then
                    idx = i 
                    ok = (pg(1)-pg(0)+1) == sz.toLong
                    break()
        )
        (idx,ok)
    // insert a new rc to pending lsit.
    private def pend(rc:ReleaseClaim):Unit = 
        val i = find((id:Long) => id >= rc.txid)
        if i >= pending.length then
            pending.append(rc)
        else
            pending.insert(i,rc)
    // search element in pending list by txid.
    private def find(fn:(Long) => Boolean):Int = 
        var l = 0
        var r = pending.length
        while l < r do 
            val m = (l+r)/2
            if fn(pending(m).txid) then
                r = m
            else
                l = m+1
        r
    //
    private def reduce(arr:ArrayBuffer[(Long,Long)]):ArrayBuffer[(Long,Long)] = 
        var mg = new ArrayBuffer[(Long,Long)]()
        var i = 0
        while i < arr.length do
            var j = i+1
            while j < arr.length && arr(j-1)(1) == arr(j)(0)-1 do
                j += 1
            if j != i+1 then
                mg.append((arr(i)(0),arr(j-1)(1)))
            else
                mg += arr(i)
            i = j 
        mg
    // merge pending and idle lists.
    private def merge():ArrayBuffer[(Long,Long)] =
        var arr = new ArrayBuffer[(Long,Long)](idle.length)
        idle.copyToBuffer(arr)
        for fc <- pending do arr ++= fc.pages
        arr.sortInPlaceWith((p1:(Long,_),p2:(Long,_)) => p1(0) < p2(0)) // TODO: use insert,not sort
        reduce(arr)
        
private object FreeArray:
    val headerSize = 9
    val elementSize = 12
    val listType:Byte = 0
    val hashType:Byte = 1
    def apply(bk:Block):Option[FreeArray] = 
        if bk.header.flag != Block.typeFreelist then
            throw new Exception(s"block type is not freelist ${bk.header.flag}") 
        bk.getBytes() match
            case None => None 
            case Some(data) => 
                var freelist = new FreeArray(bk.header)
                if data.length < headerSize then
                    throw new Exception(s"illegal freelist header data length ${data.length}") 
                bytesToHeader(data.slice(0,headerSize)) match
                    case None => throw new Exception("illegal freelist header data")
                    case Some(hd) =>
                        if data.length != headerSize+(hd.count*elementSize) then
                            throw new Exception(s"illegal freelist data length ${data.length}") 
                        
                        var idx = headerSize+elementSize
                        while idx <= data.length do
                            bytesToElem(data.slice(idx-elementSize,idx)) match
                                case None => throw new Exception("illegal freelist element data")
                                case Some(pg) =>
                                    freelist.idle += pg
                                    idx += elementSize
                        return Some(freelist)
        None 
    // parse freelist from raw bytes data.
    def apply(data:Array[Byte]):Option[FreeArray] =
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
    def bytesToHeader(data:Array[Byte]):Option[FreelistHeader] =
        if data.length != headerSize then
            throw new Exception("illegal freelist header data")
        val a = (data(0) & 0xff) << 24 | (data(1) & 0xff) << 16 | (data(2) & 0xff) << 8 | (data(3) & 0xff)
        val b = (data(4) & 0xff) << 24 | (data(5) & 0xff) << 16 | (data(6) & 0xff) << 8 | (data(7) & 0xff)
        Some(FreelistHeader((a & 0x00000000ffffffffL) << 32 | (b & 0x00000000ffffffffL),data(headerSize-1)))
    // 
    def headerToBytes(hd:FreelistHeader):Array[Byte] = 
        var c = hd.count
        var arr = new Array[Byte](headerSize)
        for i <- 0 to 7 do
            arr(7-i) = (c & 0xff).toByte
            c = c >> 8
        arr(headerSize-1) = hd.ftype
        arr
    //
    def bytesToElem(data:Array[Byte]):Option[(Long,Long)] =
        if data.length != elementSize then
            throw new Exception("illegal freelist element data")
        val a = (data(0) & 0xff) << 24 | (data(1) & 0xff) << 16 | (data(2) & 0xff) << 8 | (data(3) & 0xff)
        val b = (data(4) & 0xff) << 24 | (data(5) & 0xff) << 16 | (data(6) & 0xff) << 8 | (data(7) & 0xff) 
        val s = (a & 0x00000000ffffffffL) << 32 | (b & 0x00000000ffffffffL)
        val l = (data(8) & 0xff) << 24 | (data(9) & 0xff) << 16 | (data(10) & 0xff) << 8 | (data(11) & 0xff)
        Some((s,s+l-1))
    //
    def elemToBytes(start:Long,ed:Long):Array[Byte] = 
        var a = start>>32
        var b = start
        var c = (ed-start+1).toInt
        var arr = new Array[Byte](elementSize)
        for i <- 0 to 3 do
            arr(3-i) = (a & 0xff).toByte
            arr(7-i) = (b & 0xff).toByte
            arr(11-i) = (c & 0xff).toByte
            a = a >> 8
            b = b >> 8
            c = c >> 8
        arr



// record freelist basic info, for example, count | type
private case class FreelistHeader(count:Long,ftype:Byte)
// record a file free page fragement.
private case class FreeFragment(start:Long,end:Long,length:Int):
    override def toString(): String = s"($start,$end,$length)"
// record a file pages free claim about a version txid.
private case class FreeClaim(txid:Long, ids:ArrayBuffer[FreeFragment])

// An Inefficient Freelist Implementation.
private[platdb] class Freelist(var header:BlockHeader) extends FreeManager:
    private var prevHeader:Option[BlockHeader] = None
    // idle pages list, that can be allocated for read-write transaction.
    var idle:ArrayBuffer[FreeFragment] = new ArrayBuffer[FreeFragment]()
    //
    var unleashing:ArrayBuffer[FreeClaim] = new ArrayBuffer[FreeClaim]()
    // trace allocated pages for tx.
    var allocated:Map[Long,ArrayBuffer[FreeFragment]] = Map[Long,ArrayBuffer[FreeFragment]]() 

    override def toString(): String =
        val list = for f <- idle yield f.toString()
        list.mkString(",")
    def pageId: Long = header.pgid
    def overflow: Int = header.overflow
    /**
     * reset the freelist header id,when writable transaction commit the freelist content.
     * 
     */
    def set(pgid:Long,overflow:Int):Unit = 
        prevHeader = Some(header.clone)
        header.pgid = pgid
        header.overflow = overflow
    
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
    def release(start:Long,end:Long):Unit =
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
    def reclaim(txid:Long,start:Long,tail:Int):Unit =
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
        // rollback header.
        prevHeader match
            case None => None
            case Some(hd) => header = hd
    
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

        bk.header.flag = Block.typeFreelist
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
        if bk.header.flag != Block.typeFreelist then
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
        val a = (data(0) & 0xff) << 24 | (data(1) & 0xff) << 16 | (data(2) & 0xff) << 8 | (data(3) & 0xff)
        val b = (data(4) & 0xff) << 24 | (data(5) & 0xff) << 16 | (data(6) & 0xff) << 8 | (data(7) & 0xff)
        Some(FreelistHeader((a & 0x00000000ffffffffL) << 32 | (b & 0x00000000ffffffffL),data(headerSize-1)))
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
        val a = (data(0) & 0xff) << 24 | (data(1) & 0xff) << 16 | (data(2) & 0xff) << 8 | (data(3) & 0xff)
        val b = (data(4) & 0xff) << 24 | (data(5) & 0xff) << 16 | (data(6) & 0xff) << 8 | (data(7) & 0xff) 
        val s = (a & 0x00000000ffffffffL) << 32 | (b & 0x00000000ffffffffL)
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
