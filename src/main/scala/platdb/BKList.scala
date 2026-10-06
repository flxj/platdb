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

import scala.collection.immutable.Range
import scala.collection.mutable.{ArrayBuffer,Map}
import scala.util.{Try,Success,Failure}
import scala.util.boundary, boundary.break
import java.nio.ByteBuffer
import java.util.Base64
import scala.compiletime.ops.double

/**
  * Record basic information of the list:
  *  number of elements, 
  *  whether the identification list is reversed
  *
  * @param size
  * @param reverseFlag
  */
private class BKListInfo(var size:Long, var reverseFlag:Boolean):
    override def toString(): String = 
        Base64.getEncoder().encodeToString(toBytes())
    def toBytes(): Array[Byte] = 
        val bts = new Array[Byte](9)
        Util.longToBytes(size).copyToArray(bts)
        if reverseFlag then 
            bts(8) = 1.toByte
        bts

/**
  * An index is composed of several consecutive integer closed intervals.
  */
private[platdb] object BKList:
    val headSize:Int = 4 
    val elemSize:Int = 12
    val infoSize:Int = 9
    // keys
    val keyIndex:String = "list_index"
    val keyElems:String = "list_data"
    val keyInfo:String = "list_info"

    def apply(bk:BTreeBucket,readonly:Boolean):Option[BKList] =
        val list = new BKList(bk,readonly)
        bk.get(keyIndex) match
            case None => None
            case Some(value) => parseIndexElems(value,list.index)
        // get info, init length,reverse flag...
        bk.get(keyInfo) match
            case None => None
            case Some(value) => list.info = parseInfo(value)
        // open data bucket
        list.elems = bk.getRawBucket(keyElems) match
            case Some(bucket) => bucket
            case None => 
                if readonly then
                    return None
                bk.createRawBucketIfNotExists(keyElems) match
                    case None => throw new Exception("create list failed: create bucket error")
                    case Some(bucket) => bucket
        Some(list)
    
    def parseIndexElems(value:String,toArr:ArrayBuffer[(Long,Long,Int)]):Unit = 
        val data = Base64.getDecoder().decode(value)
        if data == null || data.length < headSize then
            return None
        val count = (data(0) & 0xff) << 24 | (data(1) & 0xff) << 16 | (data(2) & 0xff) << 8 | (data(3) & 0xff)
        if data.length != (headSize + elemSize*count) then
            return None
        for i <- 0 until count do
            val j = headSize + i*elemSize
            val a = (data(j) & 0xff) << 24 | (data(j+1) & 0xff) << 16 | (data(j+2) & 0xff) << 8 | (data(j+3) & 0xff)
            val b =  (data(j+4) & 0xff) << 24 | (data(j+5) & 0xff) << 16 | (data(j+6) & 0xff) << 8 | (data(j+7) & 0xff)
            val n = (a & 0x00000000ffffffffL) << 32 | (b & 0x00000000ffffffffL)
            val len =  (data(j+8) & 0xff) << 24 | (data(j+9) & 0xff) << 16 | (data(j+10) & 0xff) << 8 | (data(j+11) & 0xff)
            toArr.append((n,n+len-1,len))
    //
    def parseInfo(value:String):BKListInfo = 
        val data = Base64.getDecoder().decode(value)
        if data == null || data.length != infoSize then
            throw new Exception("list info format wrong") 
        else
            val info = new BKListInfo(Util.bytesToLong(data.slice(0,8)),false)
            if data(8) != 0 then 
                info.reverseFlag = true 
            info   
    //
    def indexToString(index:ArrayBuffer[(Long,Long,Int)]):String =
        var buf:ByteBuffer = ByteBuffer.allocate(headSize + index.length*elemSize)
        buf.putInt(index.length)
        for (a,_,b) <- index do
            buf.putLong(a)
            buf.putInt(b)
        Base64.getEncoder().encodeToString(buf.array())

/**
  * A BList based on bucket and ArrayBuffer implementation
  *
  * @param bk
  * @param readonly
  */
private[platdb] class BKList(bk:Bucket,readonly:Boolean) extends BList:
    private val encoder = Base64.getEncoder()
    private var index:ArrayBuffer[(Long,Long,Int)] = new ArrayBuffer[(Long,Long,Int)]()
    private var elems:RawBucket = null
    private var info:BKListInfo = new BKListInfo(0L,false)

    private def fmtKey(k:Long):Array[Byte] = Util.encodeLong(k)
    private def fmtValue(item:String):Array[Byte] = item.getBytes()
    private def fmtInfo():String = encoder.encodeToString(info.toBytes())

    private def newList(readonly:Boolean):BKList = 
        val list = new BKList(bk,readonly)
        list.elems = elems 
        list.info = info 
        list
    
    private def save():Unit = 
        bk.put(BKList.keyIndex,BKList.indexToString(index))
        bk.put(BKList.keyInfo,fmtInfo())
    
    private[platdb] def minKey:Option[Array[Byte]] = 
        if index.length == 0 then None else Some(fmtKey(index(0)(0)))
    private[platdb] def maxKey:Option[Array[Byte]] = 
        if index.length == 0 then None else Some(fmtKey(index.last(1)))

    def length: Long = info.size
    def iterator: CollectionIterator = new BKListIter(this,elems,info.reverseFlag)
    def name:String = bk.name
    def isEmpty: Boolean = info.size == 0L
    def get(idx:Int):Option[String] = 
        if idx < 0 || idx >= length then
            throw new Exception(s"index $idx out of range [0,${length})")
        elems.get(getKey(idx,info.reverseFlag)) match
            case None => None
            case Some(bs) => Some(new String(bs))

    def first:Option[String] = 
        if index.length > 0 then
            if !info.reverseFlag then 
                val (i,_,_) = index(0)
                elems.get(fmtKey(i)) match
                    case None => None
                    case Some(bs) => Some(new String(bs))
            else
                val (_,i,_) = index(index.length-1)
                elems.get(fmtKey(i)) match
                    case None => None
                    case Some(bs) => Some(new String(bs))
        else
            None
    def last:Option[String] = 
        if index.length > 0 then
            if !info.reverseFlag then
                val (_,i,_) = index(index.length-1)
                elems.get(fmtKey(i)) match
                    case None => None
                    case Some(bs) => Some(new String(bs))
            else
                val (i,_,_) = index(0)
                elems.get(fmtKey(i)) match
                    case None => None
                    case Some(bs) => Some(new String(bs))
        else 
            None
    def slice(from:Int,until:Int):Option[BList] = 
        if from < 0 || from >= length || until < 0 ||  until > length then
            throw new Exception(s"index from($from) or until($until) out of range [0,${length})")
        if from > until then
            throw new Exception(s"until($until) should larger than from($from)")
        
        val list = newList(true)
        if from == until then
            list.elems = null
            list.info.size = 0L
        else
            list.index = indexRange(info.reverseFlag,from,until-from,false)
            list.info.size = (until-from).toLong
        Some(list)

    def reverseInPlace:Unit = info.reverseFlag = !info.reverseFlag
    def reverse:BList = 
        val list = newList(readonly)
        list.index = index.clone()
        list.info.reverseFlag = !info.reverseFlag
        list
    def head:BList = 
        val list = newList(true)
        if list.index.length > 0 then
            if info.reverseFlag then
                val (i,j,n) = list.index.head
                if n <= 1 then
                    list.index = list.index.tail
                else
                    list.index(0) = (i+1,j,n-1)
            else
                val (i,j,n) = list.index.last
                if n <= 1 then
                    list.index = list.index.init
                else
                    list.index(list.index.length-1) = (i,j-1,n-1)
            list.info.size -= 1
        list
    def tail:BList = 
        val list = newList(true)
        if list.index.length > 0 then
            if !info.reverseFlag then
                val (i,j,n) = list.index.head
                if n <= 1 then
                    list.index = list.index.tail
                else
                    list.index(0) = (i+1,j,n-1)
            else
                val (i,j,n) = list.index.last
                if n <= 1 then
                    list.index = list.index.init
                else
                    list.index(list.index.length-1) = (i,j-1,n-1)
            list.info.size -= 1
        list
    def filter(pred:(String) => Boolean): BList = 
        val list = newList(true)
        var count:Long = 0L
        if !info.reverseFlag then
            for (m,n,_) <- index do
                var i = m
                while i <= n do 
                    val si = new String(elems(fmtKey(i)))
                    if pred(si) then 
                        var j = i+1
                        val sj = new String(elems(fmtKey(j-1)))
                        while j <= n && pred(sj) do     
                            j += 1
                        list.index += ((i,j-1,(j-i).toInt))
                        count += (j-i)
                        i = j
                    else
                        i += 1
        else
            for (m,n,_) <- index.reverseIterator do 
                var i = n 
                while i >= m do 
                    val si = new String(elems(fmtKey(i)))
                    if pred(si) then 
                        var j = i-1
                        val sj = new String(elems(fmtKey(j-1)))
                        while j >= m &&  pred(sj) do 
                            j -= 1
                        list.index += ((j+1,i,(i-j).toInt))
                        count += (i-j)
                    else
                        i -= 1
        list.info.size = count
        list
    def find(pred:(String) => Boolean):Int = 
        var idx = -1
        val iter = if !info.reverseFlag then iterator else reverseIterator
        boundary {
            for kv <- iter do kv match
                case None => None
                case Some((n,value)) =>
                        if pred(value) then
                            idx = n.toInt
                            break()   
        }
        idx
    def take(n: Int): Option[BList] = 
        if n < 0 || n > length then
            throw new Exception(s"parameter $n out of bound [0,${length}]")
        var list = newList(true)
        list.index =  takeIndex(info.reverseFlag,n)
        list.info.size = n.toLong
        Some(list)
    def takeRight(n: Int): Option[BList] = 
        if n < 0 || n > length then
            throw new Exception(s"parameter $n out of bound [0,${length}]")
        var list = newList(true)
        list.index = takeIndex(!info.reverseFlag,n)
        list.info.size = n.toLong
        Some(list)
    def drop(n: Int):Unit = 
        if readonly then
            throw new Exception("current list is readonly mode")
        if n < 0 || n > length then
            throw new Exception(s"parameter ${n} out of bound [0,${length}]")
        val idx = takeIndex(info.reverseFlag,n,true)
        for (m,n,_) <- idx do
            for k <- m to n do
                elems.delete(fmtKey(k))
        info.size -= n 
        save()
    def dropRight(n: Int):Unit = 
        if readonly then
            throw new Exception("current list is readonly mode")
        if n < 0 || n > length then
            throw new Exception(s"parameter ${n} out of bound [0,${length}]")
        val idx = takeIndex(!info.reverseFlag,n,true) 
        for (m,n,_) <- idx do
            for k <- m to n do
                elems.delete(fmtKey(k))
        info.size -= n 
        save()
    def insert(idx: Int, item:String):Unit = insert(idx,item)
    def insert(idx: Int, items:Seq[String]):Unit = 
        if readonly then
            throw new Exception("current list is readonly mode")
        if idx < 0 || idx >= length then
            throw new Exception(s"index $idx out of range [0,${length})")
        
        if items == null || items.length == 0 then
            return None
        if idx == 0 then
            return prepend(items)
        
        val k = shift(idx,items.length,info.reverseFlag)
        for (e,i) <- items.zipWithIndex do 
                if !info.reverseFlag then
                    elems.put(fmtKey(k+i),e.getBytes())
                else
                    elems.put(fmtKey(k-i),e.getBytes())
        val s = (
            if !info.reverseFlag then 
                (k,k+items.length-1,items.length) 
            else 
                (k-items.length+1,k,items.length)
        )
        insertIndex(s,info.reverseFlag)
        info.size += items.length
        save()
    def append(item:String):Unit = 
        if readonly then
            throw new Exception("current list is readonly mode")
        if length == 0 then
            elems.put(fmtKey(0L),item.getBytes())
            index.append((0L,0L,1))
        else
            if info.reverseFlag then
                val (m,n,l) = index(0)
                elems.put(fmtKey(m-1),item.getBytes())
                index(0) = (m-1,n,l+1)
            else
                val (m,n,l) = index.last
                elems.put(fmtKey(n+1),item.getBytes())
                index(index.length-1) = (m,n+1,l+1)
        info.size += 1
        save()
        
    def append(items:Seq[String]):Unit = 
        if readonly then
            throw new Exception("current list is readonly mode")
        if items.length == 0 then
            return None
        var r:(Long,Long,Int) = (0L,-1L,0)
        if index.length > 0 then
            r = if info.reverseFlag then index(0) else index.last
        
        for (item,i) <- items.zipWithIndex do 
            val k = if info.reverseFlag then fmtKey(r(0)-i-1) else fmtKey(r(1)+1+i)
            elems.put(k,item.getBytes())

        if index.length > 0 then
            if info.reverseFlag then
                index(0) = (r(0)-items.length,r(1),r(2)+items.length)
            else
                index(index.length-1) = (r(0),r(1)+items.length,r(2)+items.length)
        else
            if info.reverseFlag then
                index.append((-2L-items.length,-1L,items.length))
            else
                index.append((0L,0L+items.length-1,items.length))

        info.size += items.length
        save()

    def prepend(item: String):Unit = 
        if readonly then
            throw new Exception("current list is readonly mode")
        if length == 0 then
            elems.put(fmtKey(0L),item.getBytes())
            index.append((0L,0L,1))
        else
            if info.reverseFlag then
                val (m,n,l) = index.last
                elems.put(fmtKey(n+1),item.getBytes())
                index(index.length-1) = (m,n+1,l+1)
            else
                val (m,n,l) = index(0)
                elems.put(fmtKey(m-1),item.getBytes())
                index(0) = (m-1,n,l+1)
        info.size += 1
        save()
    def prepend(items: Seq[String]):Unit = 
        if readonly then
            throw new Exception("current list is readonly mode")
        if items.length == 0 then
            return None
        var r:(Long,Long,Int) = (0L,-1L,0)
        if index.length > 0 then
            r = if info.reverseFlag then index.last else index(0)
        
        for (item,i) <- items.zipWithIndex do 
            val k = if info.reverseFlag then fmtKey(r(1)+i+1) else fmtKey(r(0)-i-1)
            elems.put(k,item.getBytes())
        
        if index.length > 0 then
            if info.reverseFlag then
                index(index.length-1) = (r(0),r(1)+items.length,r(2)+items.length)
            else
                index(0) = (r(0)-items.length,r(1),r(2)+items.length)
        else
            if info.reverseFlag then
                index.append((0L,0L+items.length-1,items.length))
            else
                index.append((-2L-items.length,-1L,items.length))

        info.size += items.length
        save()

    def remove(idx: Int):Unit = remove(idx,1)
    def remove(idx: Int, count: Int):Unit = 
        if readonly then
            throw new Exception("current list is readonly mode")
        if idx < 0 || count < 0 || idx+count > length then
            throw new Exception(s"index [$idx ${idx+count}) out of range [0,${length})")
        if count == 0 then
            return None

        val sidx = indexRange(info.reverseFlag,idx,count,true)
        for (l,r,_) <- sidx do 
            for k <- l to r do 
                elems.delete(fmtKey(k))
        info.size -= count
        save()
    def set(idx: Int, item:String):Unit = 
        if readonly then
            throw new Exception("current list is readonly mode")
        if idx < 0 || idx >= length then
            throw new Exception(s"index $idx out of range [0,${length})")
        elems.put(getKey(idx,info.reverseFlag),item.getBytes()) 
    def exists(pred:(String) => Boolean): Boolean = 
        var ok:Boolean = false 
        val iter = if !info.reverseFlag then iterator else reverseIterator
        boundary {
            for kv <- iter do kv match
                case None => None
                case Some((_,value)) =>
                        if pred(value) then
                            ok = true
                            break()   
        }
        ok
    def apply(idx: Int):String = get(idx) match
        case Some(value) => value
        case None => ""
    def :+=(item:String):Unit = append(item)
    def +:=(item:String):Unit = prepend(item)
    def update(idx:Int,item:String):Unit = set(idx,item)

    private def reverseIterator:CollectionIterator = new BKListIter(this,elems,!info.reverseFlag)

    private def shift(idx:Int,count:Int,toLeft:Boolean):Long = 
        val (key,i) = findKey(idx,toLeft)
        var hole = 0L
        if !toLeft then
            // Count the holes between key intervals 
            // until the end of the index list or the 
            // hole is greater than the count parameter.
            var j = i+1
            boundary {
                while j < index.length do 
                    hole += index(j)(0)-index(j-1)(1)-1
                    if hole >= count then
                        break()
                    else
                        j += 1
            }
            var l,r:Long = 0L
            var n:Int = 0
            if hole < count then
                l = index(j-1)(1)+count-hole+1
            else
                l = index(j)(0)
                r = index(j)(1)
                n = index(j)(2)
            // Move the key to the right to free up enough space (count parameter).
            for p <- j-1 to i by -1 do
                val a = index(p)(1)
                val b = if (p == i && key > index(p)(0)) then key else index(p)(0)
                for k <- a to b by -1 do 
                    elems.get(fmtKey(k)) match
                        case None => None 
                        case Some(v) => 
                            elems.put(fmtKey(l-1),v)
                            l -= 1 
                            n += 1
            // cut off index(i) if need.
            var p = i 
            if key == index(p)(0) then
                p -= 1 
            else
                index(i) = (index(i)(0),key-1,(key-index(i)(0)).toInt)
            // Delete all keys that have already been moved.
            val s = (l,l+n-1,n)
            if hole < count then
                if p+1 < index.length then
                    index.remove(p+1,index.length-p-1)
                index.append(s)
            else
                index(j) = s 
                if p+1 < j then
                    index.remove(p+1,j-p-1)
        else
            var j = i-1
            boundary {
                while j >= 0 do 
                    hole += index(j+1)(0)-index(j)(1)-1
                    if hole >= count then
                        break()
                    else
                        j -= 1
            }
            var l,r:Long = 0L
            var n:Int = 0
            if hole < count then
                r = index(j+1)(0)+hole-count-1
            else
                l = index(j)(0)
                r = index(j)(1)
                n = index(j)(2)
            // Move the key to the left.
            for p <- j+1 to i do
                val a = index(p)(0)
                val b = if (p == i && key < index(p)(1)) then key else index(p)(1)
                for k <- a to b do 
                    elems.get(fmtKey(k)) match
                        case None => None 
                        case Some(v) => 
                            elems.put(fmtKey(r+1),v)
                            r += 1 
                            n += 1
            // cut off index(i) if need.
            var p = i 
            if key == index(p)(1) then
                p += 1 
            else
                index(i) = (key+1,index(i)(1),(index(i)(1)-key).toInt)
            // Delete all keys that have already been moved.
            val s = (r-n+1,r,n)
            if hole < count then
                if p > 0 then
                    index.remove(0,p)
                index.prepend(s)
            else
                index(j) = s 
                if p > j+1 then
                    index.remove(j+1,p-j-1)
        key
    
    private def insertIndex(s:(Long,Long,Int),right:Boolean):Unit = 
        var i:Int = -1
        boundary {
            for (p,j) <- index.zipWithIndex do
                if s(1) < p(0) then
                    if s(1) == p(0) - 1 then
                        index(j) = (s(0),p(1),s(2)+p(2))
                    else
                        i = j 
                    break()
                else 
                    if s(0) <= p(1) then
                        throw new Exception(s"index [${s(0)},${s(1)}] and [${p(0)},${p(1)}] overlapped")
                    if s(0) == p(1) + 1 then
                        index(j) = (p(0),s(1),p(2)+s(2))
                        break()
        }
        if i >= 0 then index.insert(i,s)

    private def findKey(idx:Int,right:Boolean):(Long,Int) = 
        var cnt:Int = 0
        var k:Long = 0L
        var i:Int = 0
        if !right then
            boundary {
                for ((m,_,l),j) <- index.zipWithIndex do 
                    if cnt + l >= idx then
                        k = m + idx - cnt 
                        i = j 
                        break()
                    cnt += l 
            }
        else
            boundary {
                for ((_,n,l),j) <- index.reverseIterator.zipWithIndex do 
                    if cnt + l >= idx then
                        k = n + cnt - idx 
                        i = j 
                        break()
                    cnt += l 
            }
        (k,i)
    
    private def getKey(idx:Int,right:Boolean):Array[Byte] = 
        val (k,_) = findKey(idx,right)
        fmtKey(k)

    private def takeIndex(right:Boolean,n:Int,cutFlag:Boolean = false):ArrayBuffer[(Long,Long,Int)] = 
        indexRange(right,0,n,cutFlag)

    private def indexRange(right:Boolean,idx:Int,count:Int,cutFlag:Boolean = false):ArrayBuffer[(Long,Long,Int)] = 
        val (key,i) = findKey(idx,right)
        val r = new ArrayBuffer[(Long,Long,Int)]()
        var k = key
        var j = i 
        var cnt = 0
        var split:(Long,Long,Int) = (0,0,0)
        if !right then
            boundary {
                while j < index.length do
                    val s = index(j)
                    if s(0) > k then k = s(0)
                    val d = (s(1) - k + 1).toInt
                    if cnt + d >= count then
                        val p = (k,k+(count-cnt)-1,count-cnt)
                        r.append(p)
                        if cutFlag then
                            // should split the range index(j) to two sub range.
                            if p(0) > s(0) && p(1) < s(1) then
                                index(j) = (s(0),p(0)-1,(p(0)-s(0)).toInt)
                                split = (p(1)+1,s(1),(s(1)-p(1)).toInt)
                            else if p(0) == s(0) then
                                index(j) = (p(1)+1,s(1),(s(1)-p(1)).toInt)
                            else
                                index(j) = (s(0),p(0)-1,(p(0)-s(0)).toInt)
                        cnt = count
                        break()
                    else
                        r.append((k,k+d-1,d))
                        if cutFlag then
                            index(j) = (s(0),k-1,(k-s(0)).toInt)
                        //k += d-1
                        j += 1
                        cnt += d 
            }
            if cnt != count then
                throw new Exception(s"cannot found ${count} elements at index ${idx}")
            if cutFlag then
                if split(2) != 0 then 
                    // insert at j+1
                    if j+1 < index.length then index.insert(j+1,split) else index.append(split)
                // delete elements from index[i:j+1),if its segment length <= 0
                if index(j)(2) <= 0 then j += 1
                if index(i)(2) <= 0 then
                    if j - i > 0 then
                        index.remove(i,j-i)
                else
                    if i + 1 < index.length && j-(i+1) > 0 then
                        index.remove(i+1,j-(i+1))
        else
            boundary {
                while j >= 0 do
                    val s = index(j)
                    if k > s(1) then k = s(1)
                    val d = (k - s(0)+ 1).toInt
                    if cnt + d >= count then
                        val p = (k-(count-cnt)+1,k,count-cnt)
                        r.append(p)
                        if cutFlag then
                            // should split the range index(j) to two sub range.
                            if p(0) > s(0) && p(1) < s(1) then
                                index(j) = (p(1)+1,s(1),(s(1)-p(1)).toInt)
                                split = (s(0),p(0)-1,(p(0)-s(0)).toInt)
                            else if p(0) == s(0) then
                                index(j) = (p(1)+1,s(1),(s(1)-p(1)).toInt)
                            else
                                index(j) = (s(0),p(0)-1,(p(0)-s(0)).toInt)
                        cnt = count
                        break()
                    else
                        r.append((k-d+1,k,d))
                        if cutFlag then
                            index(j) = (k+1,s(1),(s(1)-k).toInt)
                        j -= 1
                        cnt += d 
            }
            if cnt != count then
                throw new Exception(s"cannot found ${count} elements at index ${idx}")
            if cutFlag then
                if split(2) != 0 then
                    // insert at j
                    index.insert(j,split)
                    j += 1
                // delete elements from index[j:i+1),if its segment length <= 0
                if j >= 0 && j < index.length && index(j)(2) > 0 then j += 1
                if index(i)(2) <= 0 then
                    if (i+1)-j > 0 then
                        index.remove(j,(i+1)-j)
                else
                    if i - j > 0 then
                        index.remove(j,i-j)
        r

private[platdb] class BKListIter(list:BKList,bk:RawBucket,reverseFlag:Boolean) extends CollectionIterator:
    private var idx = -1L
    private var cur = bk.iterator
    def find(key:String):Option[(String,String)] = None
            
    def first():Option[(String,String)] = 
        idx = 0
        if idx >= list.length then
            return None
        if !reverseFlag then
            list.minKey match
                case None => None 
                case Some(k) => cur.find(k) match
                    case None => None
                    case Some(_,v) => Some(idx.toString(),new String(v))
        else
            list.maxKey match
                case None => None 
                case Some(k) => cur.find(k) match
                    case None => None
                    case Some(_,v) => Some(idx.toString(),new String(v))
    def last():Option[(String,String)] = 
        idx = (list.length - 1).toInt
        if idx < 0 then
            return None
        if !reverseFlag then
            list.maxKey match
                case None => None 
                case Some(k) => cur.find(k) match
                    case None => None
                    case Some(_,v) => Some(idx.toString(),new String(v))
        else
            list.minKey match
                case None => None 
                case Some(k) => cur.find(k) match
                    case None => None
                    case Some(_,v) => Some(idx.toString(),new String(v))
    def hasNext():Boolean = (idx+1) < list.length
    def next():Option[(String,String)] = 
        if !hasNext() then
            None
        else
            idx += 1
            if idx == 0 then
                first()
            else
                if !reverseFlag then 
                    cur.next() match
                        case None => None 
                        case Some(_,v) => Some(idx.toString(),new String(v))
                else
                    cur.prev() match
                        case None => None 
                        case Some(_,v) => Some(idx.toString(),new String(v))
    def hasPrev():Boolean = idx >= 1 && idx <= list.length
    def prev():Option[(String,String)] = 
        if !hasPrev() then
            None
        else
            idx -= 1
            if !reverseFlag then 
                cur.prev() match
                    case None => None 
                    case Some(_,v) => Some(idx.toString(),new String(v))
            else
                cur.next() match
                    case None => None 
                    case Some(_,v) => Some(idx.toString(),new String(v))
