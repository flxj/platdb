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
import scala.collection.mutable.ArrayBuffer
import scala.util.{Try,Success,Failure}
import scala.util.control.Breaks._
import java.nio.ByteBuffer
import java.util.Base64


/**
  * A list of strings
  */
trait BList extends Iterable:
    /**
      * name
      *
      * @return
      */
    def name:String 
    /**
      * Is the current list empty
      *
      * @return
      */
    def isEmpty: Boolean
    /**
      * Retrieve elements based on subscripts
      *
      * @param idx
      * @return
      */
    def get(idx:Int):Try[String]
    /**
      * Return list header element
      *
      * @return
      */
    def head:Try[String]
    /**
      * Return the element at the end of the list
      *
      * @return
      */
    def last:Try[String]
    /**
      * List slicing operation, obtaining a sub list in read-only mode
      *
      * @param from
      * @param until
      * @return
      */
    def slice(from:Int,until:Int):Try[BList]
    /**
      * Invert the list, and the obtained inverse list is in read-only mode
      *
      * @return
      */
    def reverse:BList
    /**
      * The list obtained after removing the end element is in read-only mode
      *
      * @return
      */
    def init:BList
    /**
      * The list obtained after removing the header element is in read-only mode
      *
      * @return
      */
    def tail:BList
    /**
      * Filter elements to obtain a read-only sublist
      *
      * @param p
      * @return
      */
    def filter(p:(String) => Boolean): BList
    /**
      * Find the index of the first element that meets the condition, and return -1 if it does not exist
      *
      * @param p
      * @return
      */
    def find(p:(String) => Boolean):Int
    /**
      * Get the first n elements, and the obtained sublist is read-only
      *
      * @param n
      * @return
      */
    def take(n: Int): Try[BList]
    /**
      * Obtain the last n elements, and the obtained sublist is read-only
      *
      * @param n
      * @return
      */
    def takeRight(n: Int): Try[BList]
    /**
      * Delete the first n elements
      *
      * @param n
      * @return
      */
    def drop(n: Int): Try[Unit]
    /**
      * Delete n elements at the end of the list
      *
      * @param n
      * @return
      */
    def dropRight(n: Int): Try[Unit]
    /**
      * Insert an element at the index position
      *
      * @param index
      * @param elem
      * @return
      */
    def insert(index: Int, elem:String): Try[Unit]
    /**
      * Insert multiple elements at index position
      *
      * @param index
      * @param elems
      * @return
      */
    def insert(index: Int, elems:Seq[String]): Try[Unit]
    /**
      * Add an element to the tail
      *
      * @param elem
      * @return
      */
    def append(elem:String):Try[Unit]
    /**
      * Add several elements to the tail
      *
      * @param elems
      * @return
      */
    def append(elems:Seq[String]):Try[Unit]
    /**
      * Insert an element into the head
      *
      * @param elem
      * @return
      */
    def prepend(elem: String):Try[Unit]
    /**
      * Insert multiple elements into the head
      *
      * @param elems
      * @return
      */
    def prepend(elems: Seq[String]):Try[Unit]
    /**
      * Delete an element
      *
      * @param index
      * @return
      */
    def remove(index: Int): Try[Unit]
    /**
      * Delete multiple elements
      *
      * @param index
      * @param count
      * @return
      */
    def remove(index: Int, count: Int):Try[Unit]
    /**
      * Update element
      *
      * @param index
      * @param elem
      * @return
      */
    def set(index: Int, elem:String): Try[Unit]
    /**
      * Does the element that meets the condition exist
      *
      * @param p
      * @return
      */
    def exists(p:(String) => Boolean): Boolean
    /**
      * Convenient methods for retrieving elements
      *
      * @param n
      * @return
      */
    def apply(n: Int):String
    /**
      * Convenient method, equivalent to append
      *
      * @param elem
      */
    def :+=(elem:String):Unit
    /**
      * Convenient method, equivalent to prepend
      *
      * @param elem
      */
    def +:=(elem:String):Unit
    /**
      * A convenient method for updating elements, equivalent to set
      *
      * @param index
      * @param elem
      */
    def update(index:Int,elem:String):Unit

private[platdb] object KList:
    val indexHeaderSize = 4
    val indexElementSize = 12
    val indexKey = "index"
    def apply(bk:Bucket,readonly:Boolean):Try[KList] =
        bk.get(indexKey) match
            case Failure(e) => 
                if DB.isNotExists(e) then
                    var list = new KList(bk,readonly)
                    list.index = ArrayBuffer[(Long,Long,Int)]()
                    Success(list)
                else 
                    Failure(e)
            case Success(value) =>
                indexElements(value) match
                    case None => Failure(new Exception("parse list index failed"))
                    case Some(idx) =>
                        var list = new KList(bk,readonly)
                        list.index = idx
                        var n = 0L
                        for (_,_,m) <- idx do
                            n+=m 
                        list.len = n
                        Success(list)
    //
    def indexElements(value:String):Option[ArrayBuffer[(Long,Long,Int)]] = 
        val data = Base64.getDecoder().decode(value)
        if data.length < indexHeaderSize then
            return None
        val count = (data(0) & 0xff) << 24 | (data(1) & 0xff) << 16 | (data(2) & 0xff) << 8 | (data(3) & 0xff)
        if data.length != (indexHeaderSize+indexElementSize*count) then
            return None
        var arr = new ArrayBuffer[(Long,Long,Int)]()
        for i <- 0 until count do
            val j = indexHeaderSize+i*indexElementSize
            val a = (data(j) & 0xff) << 24 | (data(j+1) & 0xff) << 16 | (data(j+2) & 0xff) << 8 | (data(j+3) & 0xff)
            val b =  (data(j+4) & 0xff) << 24 | (data(j+5) & 0xff) << 16 | (data(j+6) & 0xff) << 8 | (data(j+7) & 0xff)
            val n = (a & 0x00000000ffffffffL) << 32 | (b & 0x00000000ffffffffL)
            val len =  (data(j+8) & 0xff) << 24 | (data(j+9) & 0xff) << 16 | (data(j+10) & 0xff) << 8 | (data(j+11) & 0xff)
            arr+=((n,n+len-1,len))
        Some(arr)
    //
    def indexValue(index:ArrayBuffer[(Long,Long,Int)]):String =
        var buf:ByteBuffer = ByteBuffer.allocate(indexHeaderSize+index.length*indexElementSize)
        buf.putInt(index.length)
        for (a,_,b) <- index do
            buf.putLong(a)
            buf.putInt(b)
        Base64.getEncoder().encodeToString(buf.array())

/**
  * 
  *
  * @param idx
  * @param start
  * @param end
  */
private[platdb] case class IndexSlice(idx:Int,start:Long,end:Long)

/**
  * 
  *
  * @param bk
  */
private[platdb] class KList(val bk:Bucket,val readonly:Boolean) extends BList:
    var len:Long = 0L
    var index:ArrayBuffer[(Long,Long,Int)] = null
    private def copyIndex():ArrayBuffer[(Long,Long,Int)] = 
        var dest = new Array[(Long,Long,Int)](index.length)
        val _ = index.copyToArray(dest,0,index.length)
        dest.toBuffer.asInstanceOf[ArrayBuffer[(Long,Long,Int)]]
    
    def name: String = bk.name
    def length: Long = len
    def isEmpty: Boolean = len == 0L
    
    def iterator: CollectionIterator = new BListIter(this)
    
    /**
      * 
      *
      * @return
      */
    def head:Try[String] = 
        if index.length > 0 then
            val (i,_,_) = index(0)
            bk.get(formatKey(i))
        else 
            Failure(DB.exceptionListIsEmpty)
        
    /**
      * 
      *
      * @return
      */
    def last:Try[String] = 
        if index.length > 0 then
            val (_,i,_) = index(index.length-1)
            bk.get(formatKey(i))
        else 
            Failure(DB.exceptionListIsEmpty)
        
    /**
      * 
      */
    def reverse:BList = 
        var list = new KList(bk,true)
        var idx = new ArrayBuffer[(Long,Long,Int)]()
        for (i,j,n) <- index.reverseIterator do
            idx+=((j,i,n))
        list.index = idx 
        list
    
    /**
      * 
      */
    def init:BList = 
        var list = new KList(bk,true)
        if index.length > 0 then
            val (i,j,n) = index(index.length-1)
            var idx = index.init
            if n!=1 then
                idx+=((i,j-1,n-1))
            list.index = idx 
            list.len = len-1
        else 
            list.index = new ArrayBuffer[(Long,Long,Int)]()
        list
    
    /**
      * 
      */
    def tail:BList =
        var list = new KList(bk,true)
        if index.length > 0 then
            val (i,j,n) = index(0)
            var idx = index.tail
            if n!=1 then
                idx.prepend(((i+1,j,n-1)))
            list.index = idx 
            list.len = len-1
        else 
            list.index = new ArrayBuffer[(Long,Long,Int)]()
        list
    
    /**
      * 
      *
      * @param from
      * @param until
      * @return
      */
    def slice(from:Int,until:Int):Try[BList] = 
        try
            if from <0 || from >= length || until <0 ||  until >= length then
                throw new Exception(s"index from($from) or until($until) out of bound [0,${length})")
            if from >= until then
                throw new Exception(s"until($until) should larger than from($from)")
            val is = getIndexSlice(from,until-from)
            var list = new KList(bk,true)
            var idx = new ArrayBuffer[(Long,Long,Int)]()
            for s <- is do
                idx+=((s.start,s.end,(s.end-s.start+1).toInt))
            list.index = idx 
            var n = 0L
            for (_,_,m) <- idx do
                n+=m 
            list.len = n
            Success(list)
        catch
            case e:Exception => Failure(e)
    
    /**
      * 
      *
      * @param p
      */
    def filter(p:(String) => Boolean): BList = 
        var idx = new ArrayBuffer[(Long,Long,Int)]()
        for (m,n,_) <- index do
            var i = m
            while i <= n do 
                if p(bk(formatKey(i))) then 
                    var j=i+1
                    while j<=n && p(bk(formatKey(j-1))) do     
                        j+=1
                    idx+=((i,j-1,(j-i).toInt))
                    i = j
                else
                    i+=1
        var list = new KList(bk,true)
        list.index = idx
        var n = 0L
        for (_,_,m) <- idx do
            n+=m 
        list.len = n
        list
    
    /**
      * 
      *
      * @param n
      * @return
      */
    def take(n: Int): Try[BList] = 
        try
            if n < 0 || n > length then
                throw new Exception(s"$n out of bound [0,${length}]")
            val is = getIndexSlice(0,n)
            var list = new KList(bk,true)
            var idx = new ArrayBuffer[(Long,Long,Int)]()
            for s <- is do
                idx+=((s.start,s.end,(s.end-s.start+1).toInt))
            list.index = idx 
            list.len = n.toLong
            Success(list)
        catch
            case e:Exception => Failure(e)
    
    /**
      * 
      *
      * @param n
      * @return
      */
    def takeRight(n: Int): Try[BList] = 
        try
            if n < 0 || n > length then
                throw new Exception(s"$n out of bound [0,${length}]")
            val is = getIndexSlice(length-n,n)
            var list = new KList(bk,true)
            var idx = new ArrayBuffer[(Long,Long,Int)]()
            for s <- is do
                idx+=((s.start,s.end,(s.end-s.start+1).toInt))
            list.index = idx 
            list.len = n.toLong
            Success(list)
        catch
            case e:Exception => Failure(e)
    // TODO only delete index,not delete bucket elements.
    /**
      * 
      *
      * @param n
      * @return
      */
    def drop(n: Int): Try[Unit] = 
        if readonly then
            return Failure(new Exception("current list is readonly mode"))
        try
            if n < 0 || n > length then
                throw new Exception(s"$n out of bound [0,${length}]")
            val is = getIndexSlice(0,n)
            for s <- is do
                for k <- s.start to s.end do
                    bk-=(formatKey(k))
            var cp = copyIndex()
            removeIndexSlice(cp,is)
            bk+=(KList.indexKey,KList.indexValue(cp))
            index = cp
            len-=n
            Success(None)
        catch
            case e:Exception => Failure(e)
    
    /**
      * 
      *
      * @param n
      * @return
      */
    def dropRight(n: Int): Try[Unit] = 
        if readonly then
            return Failure(new Exception("current list is readonly mode"))
        try
            if n < 0 || n > length then
                throw new Exception(s"$n out of bound [0,${length}]")
            val is = getIndexSlice(length-n,n)
            for s <- is do
                for k <- s.start to s.end do
                    bk-=(formatKey(k))
            var cp = copyIndex()
            removeIndexSlice(cp,is)
            bk+=(KList.indexKey,KList.indexValue(cp))
            index = cp
            len-=n
            Success(None)
        catch
            case e:Exception => Failure(e)
    
    def find(p:(String) => Boolean):Int = 
        var idx = -1
        breakable(
            for (i,v) <- iterator do
                v match
                    case None => None
                    case Some(value) =>
                        if p(value) then
                            i match
                                case Some(n) => idx = n.toInt
                                case None => None
                            break()
        )
        idx
    /**
      * 
      *
      * @param p
      * @return
      */
    def exists(p:(String) => Boolean): Boolean = 
        var flag = false
        breakable(
            for (_,v) <- iterator do
                v match
                    case None => None
                    case Some(value) =>
                        if p(value) then
                            flag = true
                            break()
        )
        flag
    
    /**
      * 
      *
      * @param idx
      * @param elem
      * @return
      */
    def insert(idx: Int, elem:String): Try[Unit] = insert(idx,elem)

    /**
      * 
      *
      * @param idx
      * @param elems
      * @return
      */
    def insert(idx: Int, elems: Seq[String]): Try[Unit] =
        if readonly then
            return Failure(new Exception("current list is readonly mode"))
        if idx < 0 || idx >= length then
            return Failure(new Exception(s"index $idx out of bound [0,${length})"))
        if idx == 0 then
            return prepend(elems)
        else if idx == length.toInt then
            return append(elems)
        try
            if elems.length == 0 then
                return Success(None)
            var is:IndexSlice = null
            var cp = copyIndex()
            if idx > length/2 then
                shiftRight(cp,idx,elems.length) match
                    case Failure(e) => return Failure(e)
                    case Success(s) => is = s
            else 
                shiftLeft(cp,idx,elems.length) match
                    case Failure(e) => return Failure(e)
                    case Success(s) => is = s
            
            var start = is.start
            var i = 0
            for elem <- elems do
                bk+=(formatKey(start+i),elem)
                i+=1
            mergeIndexSlice(cp,is)
            bk+=(KList.indexKey,KList.indexValue(cp))
            index = cp
            len+=elems.length
            Success(None)
        catch
            case e:Exception => Failure(e)
    
    /**
      * 
      *
      * @param elem
      * @return
      */
    def prepend(elem: String):Try[Unit] = 
        if readonly then
            return Failure(new Exception("current list is readonly mode"))
        try
            var cp:ArrayBuffer[(Long,Long,Int)] = null
            if index.length > 0 then
                val (i,j,n) = index(0)
                bk+=(formatKey(i-1),elem)
                cp = copyIndex()
                cp(0) = (i-1,j,n+1)
            else 
                bk+=(formatKey(0L),elem)
                cp = ArrayBuffer[(Long,Long,Int)]((0L,0L,1))
            bk+=(KList.indexKey,KList.indexValue(cp))
            index = cp
            len+=1
            Success(None)
        catch
            case e:Exception => Failure(e)
    
    /**
      * 
      *
      * @param elem
      * @return
      */
    def append(elem: String):Try[Unit] = 
        if readonly then
            return Failure(new Exception("current list is readonly mode"))
        try
            var cp:ArrayBuffer[(Long,Long,Int)] = null
            if index.length > 0 then
                val (i,j,n) = index.last
                bk+=(formatKey(j+1),elem)
                cp = copyIndex()
                cp(cp.length-1) = (i,j+1,n+1)
            else 
                bk+=(formatKey(0L),elem)
                cp = ArrayBuffer[(Long,Long,Int)]((0L,0L,1))
            bk+=(KList.indexKey,KList.indexValue(cp))
            index = cp
            len+=1
            Success(None)
        catch
            case e:Exception => Failure(e)
    def prepend(elems:Seq[String]):Try[Unit] = 
        if readonly then
            return Failure(new Exception("current list is readonly mode"))
        if elems.length == 0 then
            return Success(None)
        try
            var cp:ArrayBuffer[(Long,Long,Int)] = null
            if index.length > 0 then
                //val start  = getKey(0)
                val (s,e,n) = index(0)
                var i = 0
                for elem <- elems do
                    i+=1
                    bk+=(formatKey(s-i),elem)
                cp = copyIndex()
                cp(0) = (s-i,e,n+elems.length)
                //mergeIndexSlice(cp,IndexSlice(0,start-i,start-1))
            else 
                var i = 0L
                for elem <- elems do
                    bk+=(formatKey(i),elem)
                    i+=1
                cp = ArrayBuffer[(Long,Long,Int)]((0L,i-1,elems.length))
            bk+=(KList.indexKey,KList.indexValue(cp))
            index = cp
            len+=elems.length
            Success(None)
        catch
            case e:Exception => Failure(e) 
    def append(elems:Seq[String]):Try[Unit] = 
        if readonly then
            return Failure(new Exception("current list is readonly mode"))
        if elems.length == 0 then
            return Success(None)
        try
            var cp:ArrayBuffer[(Long,Long,Int)] = null
            if index.length > 0 then
                val (s,e,n) = index.last
                var i = 0
                for elem <- elems do
                    i+=1
                    bk+=(formatKey(e+i),elem)
                cp = copyIndex()
                cp(cp.length-1) = (s,e+i,n+elems.length)
                //mergeIndexSlice(cp,IndexSlice(index.length-1,e+1,e+i))
            else 
                var i = 0L
                for elem <- elems do
                    bk+=(formatKey(i),elem)
                    i+=1
                cp = ArrayBuffer[(Long,Long,Int)]((0L,i-1,elems.length))
            bk+=(KList.indexKey,KList.indexValue(cp))
            index = cp
            len+=elems.length
            Success(None)
        catch
            case e:Exception => Failure(e)
    def remove(idx: Int): Try[Unit] = remove(idx,1)
    def remove(idx: Int, count: Int):Try[Unit] = 
        if readonly then
            return Failure(new Exception("current list is readonly mode"))
        if idx < 0 || count < 0 || idx+count >= length then
            return Failure(new Exception(s"index $idx ${idx+count} out of bound [0,${length})"))
        try
            if count == 0 then
                return Success(None)
            val is = getIndexSlice(idx,count)
            for s <- is do
                for k <- s.start to s.end do
                    bk-=(formatKey(k))
            var cp = copyIndex()
            removeIndexSlice(cp,is)
            bk+=(KList.indexKey,KList.indexValue(cp))
            index = cp
            len-=count
            Success(None)
        catch
            case e:Exception => return Failure(e)
    def set(idx: Int, elem:String):Try[Unit] = 
        if readonly then
            return Failure(new Exception("current list is readonly mode"))
        if idx < 0 || idx >= length then
            return Failure(new Exception(s"index $idx out of bound [0,${length})"))
        try
            bk+=(formatKey(getKey(idx)),elem) 
            Success(None)
        catch
            case e:Exception => Failure(e)
    //
    def get(idx:Int):Try[String] =
        if idx < 0 || idx >= length then
            return Failure(new Exception(s"index $idx out of bound [0,${length})"))
        bk.get(formatKey(getKey(idx)))

    //
    def update(idx: Int, elem:String):Unit = 
        if readonly then
            throw new Exception("current list is readonly mode")
        if idx < 0 || idx >= length then
            throw new Exception(s"index $idx out of bound [0,${length})")
        bk+=(formatKey(getKey(idx)),elem) 

    def apply(idx: Int):String = 
        if readonly then
            throw new Exception("current list is readonly mode")
        if idx < 0 || idx >= length then
            throw new Exception(s"index $idx out of bound [0,${length})")
        bk(formatKey(getKey(idx)))
    //
    def :+=(elem:String):Unit = 
        if readonly then
            throw new Exception("the list is readonly mode")
        var cp:ArrayBuffer[(Long,Long,Int)] = null
        if index.length > 0 then
            val (i,j,n) = index(index.length-1)
            bk+=(formatKey(j+1),elem)
            cp = copyIndex()
            cp(cp.length-1) = (i,j+1,n+1)
        else 
            bk+=(formatKey(0L),elem)
            cp = ArrayBuffer[(Long,Long,Int)]((0L,0L,1))
        bk+=(KList.indexKey,KList.indexValue(cp))
        index = cp 
        len+=1
    //
    def +:=(elem:String):Unit = 
        if readonly then
            throw new Exception("the list is readonly mode")
        var cp:ArrayBuffer[(Long,Long,Int)] = null
        if index.length > 0 then
            val (i,j,n) = index(0)
            bk+=(formatKey(i-1),elem)
            cp = copyIndex()
            cp(0) = (i-1,j,n+1)
        else 
            bk+=(formatKey(0L),elem)
            cp = ArrayBuffer[(Long,Long,Int)]((0L,0L,1))
        bk+=(KList.indexKey,KList.indexValue(cp))
        index = cp
        len+=1
    
    /**
      * 
      *
      * @param n
      * @return
      */
    private def formatKey(n:Long):String = n.toBinaryString
    /**
      * convert list index value to element key in bucket.
      *
      * @param n
      * @return
      */
    private def getKey(n:Int):Long =
        var p = 0
        var k = 0L
        breakable(
            for (i,_,m) <- index do
                if p+m >= n+1 then
                    k = n-p+i
                    break()
                p+=m
        )
        k
    
    /**
      * 
      *
      * @param arr
      * @param n
      * @return
      */
    private def getInsertIndex(arr:ArrayBuffer[(Long,Long,Int)],n:Int):(Int,Long) = 
        var p = 0
        var idx = 0
        var k = 0L
        breakable(
            for i <- 0 to arr.length do
                val (s,_,m) = arr(i)
                if p+m >= n+1 then
                    idx = i
                    k = n-p+s
                    break()
                p+=m
        )
        (idx,k)
    
    /**
      * Gets the key value of n consecutive elements starting from the specified subscript of the list.
      *
      * @param idx
      * @param count
      * @return
      */
    private def getIndexSlice(idx:Long,count:Int):List[IndexSlice] = 
        var c = 0L
        var p = 0
        var list = List[IndexSlice]()
        breakable(
            for i <- 0 until index.length do
                val (s,e,n) = index(i)
                if c > 0 then
                    if c+n >= count then
                        list:+=IndexSlice(i,s,s+count-c-1)
                        break()
                    list:+=IndexSlice(i,s,e)
                    c+=n
                else
                    if p+n >= idx+1 then
                        val k=idx-p+s
                        if k+count-1 <= e then
                            list:+= IndexSlice(i,k,k+count-1)
                            break()
                        list:+=IndexSlice(i,k,e)
                        c = e-k+1
                    else
                        p+=n
        )
        list

    /**
      * Remove these index fragments from the index.
      *
      * @param arr
      * @param s
      */
    private def removeIndexSlice(arr:ArrayBuffer[(Long,Long,Int)],s:List[IndexSlice]):Unit = 
        for is <- s.reverse do
            val (s,e,n) = arr(is.idx)
            if is.start <= s then
                if is.end >= e then
                    arr.remove(is.idx)
                else 
                    if is.end >= s then
                        arr(is.idx) = (is.end+1,e,(e-is.end).toInt)
            else 
                if is.start <= e then
                    arr(is.idx) = (s,is.start-1,(is.start-s).toInt)
                    if is.end < e then
                        if is.idx+1 < arr.length then
                            arr.insert(is.idx+1, (is.end+1,e,(e-is.end).toInt))
                        else
                            arr+=((is.end+1,e,(e-is.end).toInt))
    private def min(a:Long,b:Long):Long = if a>=b then b else a
    private def max(a:Long,b:Long):Long = if a>=b then a else b

    /**
      * 
      *
      * @param arr
      * @param is
      */
    private def mergeIndexSlice(arr:ArrayBuffer[(Long,Long,Int)],is:IndexSlice):Unit = 
        if is.idx <0 || is.idx >= arr.length then
            return None
        val (s,e,_) = arr(is.idx)
        if is.end < s-1 || is.start > e+1 then
            return None
        val m = min(is.start,s)
        val n = max(is.end,e)
        if m > n then
            return None
        arr(is.idx) = (m,n,(n-m+1).toInt)
        // merge adjacent intervals.
        var newIdx = new ArrayBuffer[(Long,Long,Int)]()
        var i = 0
        while i<arr.length do
            var j = i+1
            var continue = true
            while j<arr.length && continue do
                val (_,e,_) = arr(j-1)
                val (s,_,_) = arr(j)
                if e >= s || e == s-1 then
                    j+=1
                else
                    continue = false
            if j!=i+1 then
                val (s,_,_) = arr(i)
                val (_,e,_) = arr(j-1)
                newIdx+=((s,e,(e-s+1).toInt))
            else
                newIdx+=arr(i)
            i = j 
        arr.clear()
        arr ++= newIdx
    
    /**
      * 
      *
      * @param arr
      * @param idx
      * @param key
      * @return
      */
    private def splitIndex(arr:ArrayBuffer[(Long,Long,Int)],idx:Int,key:Long):(ArrayBuffer[(Long,Long,Int)],ArrayBuffer[(Long,Long,Int)]) =
        var arr1 = ArrayBuffer[(Long,Long,Int)]()
        var arr2 = ArrayBuffer[(Long,Long,Int)]()
        if idx+1 < arr.length then
            arr2 = arr.slice(idx+1,arr.length)
        if idx > 0 then
            arr1 = arr.slice(0,idx)
        
        val (s,e,n) = arr(idx)
        if key == s then
            arr2.prepend((s,e,n))
        else 
            arr1.append((s,key-1,(key-s).toInt))
            arr2.prepend((key,e,(e-key).toInt))
        (arr1,arr2)
    
    /**
      * 
      *
      * @param arr
      * @param idx
      * @param count
      * @return
      */
    private def shiftLeft(arr:ArrayBuffer[(Long,Long,Int)],idx:Int,count:Int):Try[IndexSlice] = 
        val (ii,k) = getInsertIndex(arr,idx)
        // split the raw index,and we just need shift the first half.
        var (arr1,arr2) = splitIndex(arr,ii,k)
        // shift left the arr1 in order to obtain count consecutive free positions.
        // use move array to record moving step for arr1 elements.
        var move = ArrayBuffer[Int](count)
        var i = arr1.length-2
        while i>=0 do
            val (ps,_,_) = arr1(i+1)
            val m = move(arr1.length-i-2)
            val (_,e,_) = arr1(i)
            if ps-m > e then
                // not need to shift current index element.
                i = -1
            else
                move += (e-ps+m+1).toInt
                i-=1
        // move bucket elements.
        try
            for j <- Range(move.length-1,0,-1) do
                val (s,e,n) = arr1(arr1.length-j-1)
                val m = move(j)
                var k = s
                while k <= e do 
                    bk+=(formatKey(k-m),bk(formatKey(k)))
                    k+=1
                arr2(j) = (s-m,e-m,n)
            arr.clear()
            arr++=arr1
            arr++=arr2
            Success(IndexSlice(ii,k-count,k-1))
        catch
            case e:Exception => Failure(e)

    /**
      * 
      *
      * @param arr
      * @param idx
      * @param count
      * @return
      */
    private def shiftRight(arr:ArrayBuffer[(Long,Long,Int)],idx:Int,count:Int):Try[IndexSlice] = 
        val (ii,k) = getInsertIndex(arr,idx)
        // split the raw index,and we just need shift the second half.
        var (arr1,arr2) = splitIndex(arr,ii,k)
        // shift right the arr2 in order to obtain count consecutive free positions.
        // use move array to record moving step for arr2 elements.
        var move = ArrayBuffer[Int](count)
        var i = 1
        while i<arr2.length do
            val (_,pe,_) = arr2(i-1)
            val m = move(i-1)
            val (s,_,_) = arr2(i)
            if pe+m < s then
                // not need to shift current index element.
                i = arr2.length
            else
                move += (pe+m-s+1).toInt
                i+=1
        // move bucket elements.
        try
            for j <- Range(move.length-1,0,-1) do
                val (s,e,n) = arr2(j)
                val m = move(j)
                var k = e
                while k >= s do 
                    bk+=(formatKey(k+m),bk(formatKey(k)))
                    k-=1
                arr2(j) = (s+m,e+m,n)
            arr.clear()
            arr++=arr1
            arr++=arr2
            Success(IndexSlice(ii,k,k+count-1))
        catch
            case e:Exception => Failure(e)

private[platdb] class KListIter(val list:KList) extends CollectionIterator:
    def find(key:String):(Option[String],Option[String]) = ???
    def first():(Option[String],Option[String]) = ???
    def last():(Option[String],Option[String]) = ???
    def hasNext():Boolean = ???
    def next():(Option[String],Option[String]) = ???
    def hasPrev():Boolean = ???
    def prev():(Option[String],Option[String]) = ???


class BListIter(val list:BList) extends CollectionIterator:
    private var idx = 0
    def find(key:String):(Option[String],Option[String]) = (None,None)
    def first():(Option[String],Option[String]) = 
        idx = 0
        list.head match
            case Failure(_) => (None,None)
            case Success(v) => (Some("0"),Some(v))
    def last():(Option[String],Option[String]) = 
        idx = list.length.toInt
        list.last match
            case Failure(_) => (None,None)
            case Success(v) => (Some((list.length-1).toString),Some(v))
    def hasNext():Boolean = idx < list.length
    def next():(Option[String],Option[String]) = 
        list.get(idx) match
            case Failure(_) => (None,None)
            case Success(v) => 
                idx+=1
                (Some(idx.toString),Some(v))
    def hasPrev():Boolean = idx >= 0
    def prev():(Option[String],Option[String]) = 
        list.get(idx) match
            case Failure(_) => (None,None)
            case Success(v) => 
                idx-=1
                (Some(idx.toString),Some(v))

