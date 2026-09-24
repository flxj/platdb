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
import scala.collection.mutable.{ArrayBuffer,Map,SortedMap}
import scala.jdk.CollectionConverters.*
import scala.util.control.Breaks._
import scala.util.{Try,Success,Failure}
import scala.compiletime.ops.double
import java.util.Comparator

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

// record freelist basic info, for example, count | type
private case class FreelistHeader(count:Long,ftype:Byte)
// record a file free page fragement.
private class FreeFragment(var start:Long,var end:Long,var length:Int) extends DoubleLinkedNode[FreeFragment]:
    override def toString(): String = s"($start,$end,$length)"
    def reset(s:Long,e:Long):Unit =
        start = s 
        end = e 
        length = (e-s+1).toInt
    def overlap(f:FreeFragment):Boolean = !(end < f.start || f.end < start)

private object FreeList:
    val headSize = 9
    val elemSize = 12
    val listType:Byte = 0
    val hashType:Byte = 1
    // parse freelist from block data.
    def apply(bk:Block):Option[FreeList] = 
        if bk.header.flag != Block.typeFreelist then
            throw new Exception(s"block type is not freelist ${bk.header.flag}") 
        bk.getBytes() match
            case None => None 
            case Some(data) => 
                var freelist = new FreeList(bk.header)
                if data.length < headSize then
                    throw new Exception(s"illegal freelist header data length ${data.length}") 
                FreeList.bytesToHeader(data.slice(0,headSize)) match
                    case None => throw new Exception("illegal freelist header data")
                    case Some(hd) =>
                        if data.length != headSize+(hd.count*elemSize) then
                            throw new Exception(s"illegal freelist data length ${data.length},except ${headSize+(hd.count*elemSize)}") 
                        
                        var idx = headSize+elemSize
                        while idx <= data.length do
                            FreeList.bytesToElem(data.slice(idx-elemSize,idx)) match
                                case None => throw new Exception("illegal freelist element data")
                                case Some(st,ed) =>
                                    val ff = new FreeFragment(st,ed,(ed-st).toInt)
                                    freelist.idle.pushTail(ff)
                                    idx += elemSize
                        freelist.reorder(false)
                        Some(freelist)
    // parse freelist from raw bytes data.
    def apply(data:Array[Byte]):Option[FreeList] =
        if data.length < BlockHeader.size + FreeList.headSize then
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
        if data.length != headSize then
            throw new Exception("illegal freelist header data")
        val a = (data(0) & 0xff) << 24 | (data(1) & 0xff) << 16 | (data(2) & 0xff) << 8 | (data(3) & 0xff)
        val b = (data(4) & 0xff) << 24 | (data(5) & 0xff) << 16 | (data(6) & 0xff) << 8 | (data(7) & 0xff)
        Some(FreelistHeader((a & 0x00000000ffffffffL) << 32 | (b & 0x00000000ffffffffL),data(headSize-1)))
    // 
    def headerToBytes(hd:FreelistHeader):Array[Byte] = 
        var c = hd.count
        var arr = new Array[Byte](headSize)
        for i <- 0 to 7 do
            arr(7-i) = (c & 0xff).toByte
            c = c >> 8
        arr(headSize-1) = hd.ftype
        arr
    //
    def bytesToElem(data:Array[Byte]):Option[(Long,Long)] =
        if data.length != elemSize then
            throw new Exception("illegal freelist element data")
        val a = (data(0) & 0xff) << 24 | (data(1) & 0xff) << 16 | (data(2) & 0xff) << 8 | (data(3) & 0xff)
        val b = (data(4) & 0xff) << 24 | (data(5) & 0xff) << 16 | (data(6) & 0xff) << 8 | (data(7) & 0xff) 
        val s = (a & 0x00000000ffffffffL) << 32 | (b & 0x00000000ffffffffL)
        val l = (data(8) & 0xff) << 24 | (data(9) & 0xff) << 16 | (data(10) & 0xff) << 8 | (data(11) & 0xff)
        Some((s,s+l-1))
    
    def elemToBytes(start:Long,ed:Long):Array[Byte] = elemToBytes(start,(ed-start+1).toInt)
    
    def elemToBytes(start:Long,len:Int):Array[Byte] = 
        var a = start>>32
        var b = start
        var c = len
        var arr = new Array[Byte](elemSize)
        for i <- 0 to 3 do
            arr(3-i) = (a & 0xff).toByte
            arr(7-i) = (b & 0xff).toByte
            arr(11-i) = (c & 0xff).toByte
            a = a >> 8
            b = b >> 8
            c = c >> 8
        arr

private object FreeArray:
    def apply(bk:Block):Option[FreeArray] = 
        val hSize = FreeList.headSize
        val eSize = FreeList.elemSize
        //
        if bk.header.flag != Block.typeFreelist then
            throw new Exception(s"block type is not freelist ${bk.header.flag}") 
        bk.getBytes() match
            case None => None 
            case Some(data) => 
                var freelist = new FreeArray(bk.header)
                if data.length < hSize then
                    throw new Exception(s"illegal freelist header data length ${data.length}") 
                FreeList.bytesToHeader(data.slice(0,hSize)) match
                    case None => throw new Exception("illegal freelist header data")
                    case Some(hd) =>
                        if data.length != hSize+(hd.count*eSize) then
                            throw new Exception(s"illegal freelist data length ${data.length}") 
                        
                        var idx = hSize+eSize
                        while idx <= data.length do
                            FreeList.bytesToElem(data.slice(idx-eSize,idx)) match
                                case None => throw new Exception("illegal freelist element data")
                                case Some(pg) =>
                                    freelist.idle += pg
                                    idx += eSize
                        return Some(freelist)
        None 
    // parse freelist from raw bytes data.
    def apply(data:Array[Byte]):Option[FreeArray] =
        if data.length < BlockHeader.size + FreeList.headSize then
            throw new Exception(s"illegal freelist data length ${data.length}")
        BlockHeader(data.slice(0,BlockHeader.size)) match
            case None => throw new Exception("parse freelist block header data failed")
            case Some(hd) =>
                var bk = new Block(data.length)
                bk.header = hd
                bk.write(0,data)
                apply(bk)

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
        var sz = BlockHeader.size + FreeList.headSize + idle.length*FreeList.elemSize
        for p <- pending do
            sz += p.pages.length*FreeList.elemSize
        sz 
    
    def writeTo(bk:Block):Int =
        val pgids = merge()
        val sz = BlockHeader.size+FreeList.headSize+pgids.length*FreeList.elemSize

        bk.header.flag = Block.typeFreelist
        bk.header.count = 1
        bk.header.size = sz
        bk.header.overflow =(sz+DB.pageSize)/DB.pageSize - 1
        
        bk.append(bk.header.getBytes())
        bk.append(FreeList.headerToBytes(FreelistHeader(pgids.length,FreeList.listType)))
        for (s,e) <- pgids do
            bk.append(FreeList.elemToBytes(s,e))
        sz
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
        if n <= 0 then
            throw new Exception(s"allocate negative page n:${n}")
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
        arr.asJava.sort(new Comparator[(Long,Long)]{
            override def compare(o1: (Long, Long), o2: (Long, Long)): Int = (o1(0)-o2(0)).toInt
        })
        //arr.sortInPlaceWith((p1:(Long,_),p2:(Long,_)) => p1(0) < p2(0)) // TODO: use insert,not sort
        reduce(arr)

// An double linked list Implementation.
private[platdb] class FreeList(var header:BlockHeader) extends FreeManager:
    private var prevHeader:Option[BlockHeader] = None
    // idle pages list, that can be allocated for read-write transaction.
    var num:Long = 0
    var idle:DoubleLinkedList[FreeFragment] = new DoubleLinkedList[FreeFragment]()
    // record a file data pages freeclaim about a version txid.
    var pending:SortedMap[Long,ArrayBuffer[FreeFragment]] = SortedMap[Long,ArrayBuffer[FreeFragment]]()
    // trace allocated pages for tx.
    var allocated:Map[Long,ArrayBuffer[FreeFragment]] = Map[Long,ArrayBuffer[FreeFragment]]() 

    override def toString(): String =
        (for f <- idle.iterator yield f.toString()).mkString(",")
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
      * the next write transaction will try to call the Freelist Unleash method to 
      * free the pages in the pending before starting execution, 
      * and as long as the version to be released in the pending is less than the 
      * minimum version held by the currently open read-only transaction, 
      * then the pending elements can be released (indicating that there are definitely 
      * no read-only transactions holding the pending pages anymore)
      * at the same time, the version held by the currently open read-only transaction 
      * may span a large span, so for pages between two adjacent versions, 
      * if no transaction is already holding it, then it can also be released
      *
      * @param start
      * @param end
      */
    def release(start:Long,end:Long):Unit =
        if start > end then 
            return None
        
        for (id,ff) <- pending.range(start,end+1) do
            pending.remove(id) match
                case None => None
                case Some(ff) => 
                    for f <- ff do idle.pushHead(f)
                    num += ff.length
            allocated.remove(id)

        if num > 0 && 2*num > idle.length then
            reorder(false)
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
        val ed = start+tail
        for f <- idle.iterator do
            if !(ed < f.start || f.end < start) then
                throw new Exception(s"release repeatedly,tx $txid try to release [$start,$ed], but already released [${f.start},${f.end}]")
        for (id,ffs) <- pending do 
            for f <- ffs do if !(ed < f.start || f.end < start) then
                throw new Exception(s"release repeatedly,tx $txid try to release [$start,$ed], but tx ${id} already released [${f.start},${f.end}]")
        //
        val f = FreeFragment(start,ed,tail+1)
        pending.get(txid) match
            case Some(ffs) => ffs.append(f)
            case None => pending.put(txid,ArrayBuffer[FreeFragment](f))

    /**
      * Allocate contiguous space of size n*osPageSize and return the 
      * id of the first page, if there is no space in the current idle 
      * list that meets the conditions, it will return -1(in this case, 
      * you need to allocate space from the end of the file and grow the file).
      *
      * @param txid
      * @param n
      * @return pgid
      */
    def allocate(txid:Long,n:Int):Long = 
        if n <= 0 then
            throw new Exception(s"allocate negative page n:${n}")
        var f:FreeFragment = null
        breakable(
            for ff <- idle.iterator do 
                if ff.length >= n then
                    f = ff 
                    break()
        )
        if f == null then
            return -1
        else
            var fr:FreeFragment = null
            if f.length == n then
                idle.remove(f)
                fr = f 
            else
                f.start += n
                f.length = (f.end-f.start+1).toInt
                fr = new FreeFragment(f.start-n,f.start-1,n)
            // move the fragment to allocated set.
            allocated.get(txid) match
                case None => allocated.put(txid,ArrayBuffer[FreeFragment](fr))
                case Some(ffs) => 
                    // check
                    for f <- ffs do if !( fr.end < f.start || f.end < fr.start) then
                        throw new Exception(s"allocate repeatedly,tx $txid try to allocate [${fr.start},${fr.end}], but tx $txid already allocated [${f.start},${f.end}]")
                    ffs.append(fr)
            fr.start
    /**
      * rollback pages release/allocate operations about txid.
      *
      * @param txid
      */
    def rollback(txid:Long):Unit =
        // return the assigned pages to the idle list.
        allocated.remove(txid) match
            case None => None
            case Some(ffs) =>
                for f <- ffs do idle.pushHead(f)
                num += ffs.length
        
        // retract the statement about the release of pages.
        pending.remove(txid)
        
        // rollback header.
        prevHeader match
            case None => None
            case Some(hd) => header = hd
        
        if num > 0 && 2*num > idle.length then
            reorder(false)
    /**
      * merge FreeFragment array elements and sort it by pgid.
      *
      * @param arr
      * @return
      */
    private def reorder(merge:Boolean):Unit =
        var arr = new ArrayBuffer[FreeFragment]()
        for f <- idle.iterator do arr.append(f)
        if merge then
            for (_,ffs) <- pending do
                for f <- ffs do arr.append(f)
        // sort by pgid
        arr.asJava.sort(new Comparator[FreeFragment] {
            override def compare(o1: FreeFragment, o2: FreeFragment): Int = (o1.start - o2.start).toInt
        })
        // merge
        val list = new DoubleLinkedList[FreeFragment]()
        var i = 0
        while i < arr.length do
            var j = i+1
            while j < arr.length && arr(j-1).end == arr(j).start-1 do
                j += 1
            if j != i+1 then
                val f = new FreeFragment(arr(i).start,arr(j-1).end,(arr(j-1).end-arr(i).start+1).toInt)
                list.pushTail(f)
            else
                list.pushTail(arr(i))
            i = j 
        idle = list
        num = 0
    //
    private def count():Int = 
        var cnt = idle.length
        for (_,ffs) <- pending do cnt += ffs.length
        cnt
    def size():Int = 
        var sz = BlockHeader.size + FreeList.headSize + idle.length*FreeList.elemSize
        for (_,ffs) <- pending do
            sz += ffs.length*FreeList.elemSize
        sz 
    def writeTo(bk:Block):Int =
        val sz = size()
        val cnt = count()
        bk.header.flag = Block.typeFreelist
        bk.header.count = 1
        bk.header.size = sz
        bk.header.overflow = (sz+DB.pageSize)/DB.pageSize - 1
        
        bk.append(bk.header.getBytes())
        bk.append(FreeList.headerToBytes(FreelistHeader(cnt,FreeList.listType)))
        for ff <- idle.iterator do
            bk.append(FreeList.elemToBytes(ff.start,ff.end))
        for (_,ffs) <- pending do 
            for ff <- ffs do 
                bk.append(FreeList.elemToBytes(ff.start,ff.end))
        sz

private object FreeTree:
    def apply(bk:Block):Option[FreeTree] = 
        val hSize = FreeList.headSize
        val eSize = FreeList.elemSize
        if bk.header.flag != Block.typeFreelist then
            throw new Exception(s"block type is not freelist ${bk.header.flag}") 
        bk.getBytes() match
            case None => None 
            case Some(data) => 
                var freetree = new FreeTree(bk.header)
                if data.length < hSize then
                    throw new Exception(s"illegal freelist header data length ${data.length}") 
                FreeList.bytesToHeader(data.slice(0,hSize)) match
                    case None => throw new Exception("illegal freelist header data")
                    case Some(hd) =>
                        if data.length != hSize+(hd.count*eSize) then
                            throw new Exception(s"illegal freelist data length ${data.length},except ${hSize+(hd.count*eSize)}") 
                        
                        var idx = hSize+eSize
                        while idx <= data.length do
                            FreeList.bytesToElem(data.slice(idx-eSize,idx)) match
                                case None => throw new Exception("illegal freelist element data")
                                case Some(st,ed) =>
                                    val ff = new FreeFragment(st,ed,(ed-st).toInt)
                                    freetree.idle.put(ff.start,ff)
                                    idx += eSize
                        Some(freetree)
    // parse freelist from raw bytes data.
    def apply(data:Array[Byte]):Option[FreeTree] =
        if data.length < BlockHeader.size + FreeList.headSize then
            throw new Exception(s"illegal freelist data length ${data.length}")
        BlockHeader(data.slice(0,BlockHeader.size)) match
            case None => throw new Exception("parse freelist block header data failed")
            case Some(hd) =>
                var bk = new Block(data.length)
                bk.header = hd
                bk.write(0,data)
                apply(bk)

private[platdb] class FreeTree(var header:BlockHeader) extends FreeManager:
    private var prevHeader:Option[BlockHeader] = None
    // idle pages list, that can be allocated for read-write transaction.
    var idle:SkipList[Long,FreeFragment] = new SkipList[Long,FreeFragment](8)
    // record a file data pages freeclaim about a version txid.
    var pending:SortedMap[Long,ArrayBuffer[FreeFragment]] = SortedMap[Long,ArrayBuffer[FreeFragment]]()
    // trace allocated pages for tx.
    var allocated:Map[Long,ArrayBuffer[FreeFragment]] = Map[Long,ArrayBuffer[FreeFragment]]() 

    override def toString(): String =
        (for f <- idle.iterator yield f.toString()).mkString(",")
    def pageId: Long = header.pgid
    def overflow: Int = header.overflow
    def set(pgid:Long,overflow:Int):Unit = 
        prevHeader = Some(header.clone)
        header.pgid = pgid
        header.overflow = overflow
    def release(start:Long,end:Long):Unit =
        if start > end then 
            return None
        
        for (id,ff) <- pending.range(start,end+1) do
            pending.remove(id) match
                case None => None
                case Some(ff) => 
                    for f <- ff do 
                        val node = new SkipListNode[Long,FreeFragment](0,f.start,f)
                        idle.putNode(node)
                        merge(node)
            allocated.remove(id)
    def reclaim(txid:Long,start:Long,tail:Int):Unit = 
        val ed = start + tail
        val f = FreeFragment(start,ed,tail+1)
        var node:SkipListNode[Long,FreeFragment] = null
        idle.getPrevNode(start) match
            case None => None
            case Some(n) => 
                node = n 
                breakable(
                    while node != null do 
                        if f.overlap(node.value) then
                            throw new Exception(s"release repeatedly,tx $txid try to release [$start,$ed],but its overlap with already released")
                        else
                            if node.value.start > f.end then
                                break()
                            node.next(0) match
                                case None => break()
                                case Some(n) => node = n 
                )
        for (id,ffs) <- pending do 
            for ff <- ffs do if ff.overlap(f) then
                throw new Exception(s"release repeatedly,tx $txid try to release [$start,$ed], but tx ${id} already released [${ff.start},${ff.end}]")
        //
        pending.get(txid) match
            case Some(ffs) => ffs.append(f)
            case None => pending.put(txid,ArrayBuffer[FreeFragment](f))

    def allocate(txid:Long,n:Int):Long = 
        if n <= 0 then
            throw new Exception(s"allocate negative page n:${n}")
        var node:SkipListNode[Long,FreeFragment] = null
        breakable(
            for ff <- idle.nodeIterator do 
                if ff.value.length >= n then
                    node = ff 
                    break()
        )
        if node == null then
            return -1
        else
            var fr:FreeFragment = null
            if node.value.length == n then
                idle.remove(node.key)
                fr = node.value 
            else
                node.value.reset(node.value.start+n,node.value.end)
                fr = new FreeFragment(node.value.start-n,node.value.start-1,n)
            // move the fragment to allocated set.
            allocated.get(txid) match
                case None => allocated.put(txid,ArrayBuffer[FreeFragment](fr))
                case Some(ffs) => 
                    // check
                    for f <- ffs do if f.overlap(fr) then
                        throw new Exception(s"allocate repeatedly,tx $txid try to allocate [${fr.start},${fr.end}], but tx $txid already allocated [${f.start},${f.end}]")
                    ffs.append(fr)
            fr.start
    def rollback(txid:Long):Unit =
        // return the assigned pages to the idle list.
        allocated.remove(txid) match
            case None => None
            case Some(ffs) =>
                for f <- ffs do 
                    val node = new SkipListNode[Long,FreeFragment](0,f.start,f)
                    idle.putNode(node)
                    merge(node)
        
        // retract the statement about the release of pages.
        pending.remove(txid)
        
        // rollback header.
        prevHeader match
            case None => None
            case Some(hd) => header = hd
    //
    private def merge(node:SkipListNode[Long,FreeFragment]):Unit = 
        val keys = new ArrayBuffer[Long]()
        var ed = node.value.end
        var nxt = node.next(0)
        breakable(
            while true do 
                nxt match
                    case None => break()
                    case Some(n) =>
                        if n.value.start == ed+1 then
                            ed = n.value.end
                            keys.append(n.key)
                            nxt = n.next(0)
                        else
                            break()
        )
        if keys.length > 0 then
            node.value.reset(node.key,ed)
            for k <- keys do idle.remove(k)
    //
    private def reform(merge:Boolean):Unit =
        var arr = new ArrayBuffer[FreeFragment]()
        for f <- idle.iterator do arr.append(f)
        if merge then
            for (_,ffs) <- pending do
                for f <- ffs do arr.append(f)
        // sort by pgid
        arr.asJava.sort(new Comparator[FreeFragment] {
            override def compare(o1: FreeFragment, o2: FreeFragment): Int = (o1.start - o2.start).toInt
        })
        // merge
        val list = new SkipList[Long,FreeFragment]()
        var i = 0
        while i < arr.length do
            var j = i+1
            while j < arr.length && arr(j-1).end == arr(j).start-1 do
                j += 1
            if j != i+1 then
                val f = new FreeFragment(arr(i).start,arr(j-1).end,(arr(j-1).end-arr(i).start+1).toInt)
                list.put(f.start,f)
            else
                list.put(arr(i).start,arr(i))
            i = j 
        idle = list
    //
    private def count():Int = 
        var cnt = idle.length
        for (_,ffs) <- pending do cnt += ffs.length
        cnt
    def size():Int = 
        var sz = BlockHeader.size + FreeList.headSize + idle.length*FreeList.elemSize
        for (_,ffs) <- pending do
            sz += ffs.length*FreeList.elemSize
        sz 
    def writeTo(bk:Block):Int =
        val sz = size()
        val cnt = count()
        bk.header.flag = Block.typeFreelist
        bk.header.count = 1
        bk.header.size = sz
        bk.header.overflow = (sz+DB.pageSize)/DB.pageSize - 1
        
        bk.append(bk.header.getBytes())
        bk.append(FreeList.headerToBytes(FreelistHeader(cnt,FreeList.listType)))
        for ff <- idle.iterator do
            bk.append(FreeList.elemToBytes(ff.start,ff.end))
        for (_,ffs) <- pending do 
            for ff <- ffs do 
                bk.append(FreeList.elemToBytes(ff.start,ff.end))
        sz
