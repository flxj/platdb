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

import scala.collection.mutable.{ArrayBuffer,Map}
import scala.util.{Try,Failure,Success}
import scala.util.control.Breaks._
import java.nio.ByteBuffer
import java.util.Base64

    
/**
  * spatial index
  */
trait Region:
    /**
      * name
      *
      * @return
      */
    def name:String
    /**
      * number of objects.
      *
      * @return
      */
    def length:Long
    /**
      * the dimension of current region.
      *
      * @return
      */
    def dimension:Int
    /**
      * Iterate all objects in ascending order according to the key value dictionary.
      *
      * @return
      */
    def iterator:Iterator[SpatialObject]
    /**
      * Insert a spatial point object into the current region
      *
      * @param coordinate
      * @param key
      * @param Value
      * @return
      */
    def mark(coordinate:Array[Double],key:String,Value:String):Try[Unit]
    /**
      * Insert a spatial object into the current region
      *
      * @param obj
      * @return
      */
    def put(obj:SpatialObject):Try[Unit]
    /**
      * Query spatial object information
      *
      * @param key
      * @return
      */
    def get(key:String):Try[SpatialObject]
    /**
      * Query and filter all objects that intersect with the given query window
      *
      * @param range
      * @param filter
      * @return
      */
    def search(range:Rectangle,filter:(SpatialObject)=>Boolean):Try[Seq[SpatialObject]]
    /**
      * Filter all objects in the entire area
      *
      * @param filter
      * @return
      */
    def scan(filter:(SpatialObject)=>Boolean):Try[Seq[SpatialObject]]
    /**
      * Delete a spatial object
      *
      * @param key
      * @return
      */
    def delete(key:String):Try[Unit]
    /**
      * Filter and delete objects within the specified range (intersecting with the query window)
      *
      * @param range
      * @param filter
      * @return
      */
    def delete(range:Rectangle,filter:(SpatialObject)=>Boolean):Try[Unit]
    /**
      * Delete all objects containing range (completely covering the query window)
      *
      * @param range
      * @return
      */
    def delete(range:Rectangle):Try[Unit] 
    /**
      * Returns the range of the entire region
      *
      * @return
      */
    def boundary():Try[Rectangle]
    /**
      * 
      *
      * @param obj
      * @param filter
      * @param distFunc
      * @return
      */
    def nearby(obj:SpatialObject,filter:(SpatialObject)=>Boolean)(using distFunc:(SpatialObject,SpatialObject)=>Double):Try[Seq[(SpatialObject,Double)]]
    /**
      * Query k objects closest to obj object.
      *
      * @param obj
      * @param k
      * @param distFunc
      * @return
      */
    def nearby(obj:SpatialObject,k:Int)(using distFunc:(SpatialObject,SpatialObject)=>Double):Try[Seq[(SpatialObject,Double)]]
    /**
      * Query the k objects closest to the key object
      *
      * @param key
      * @param k
      * @param distFunc
      * @return
      */
    def nearby(key:String,k:Int)(using distFunc:(SpatialObject,SpatialObject)=>Double):Try[Seq[(SpatialObject,Double)]]
    /**
      * Query all other objects with a distance less than or equal to d from the obj object
      *
      * @param obj
      * @param d
      * @param limit
      * @param distFunc
      * @return
      */
    def nearby(obj:SpatialObject,d:Double,limit:Int)(using distFunc:(SpatialObject,SpatialObject)=>Double):Try[Seq[(SpatialObject,Double)]]
    /**
      * A convenient method for adding spatial objects, equivalent to putting
      *
      * @param obj
      * @throws
      */
    def +=(obj:SpatialObject):Unit
    /**
      * A convenient method for deleting spatial objects, equivalent to delete
      *
      * @param key
      */
    def -=(key:String):Unit
    /**
      * Convenient methods for updating objects
      *
      * @param key
      * @param obj
      * @throws
      */
    def update(key:String,obj:SpatialObject):Unit
/**
  * 
  *
  * @param obj1
  * @param obj2
  * @return
  */
def centreDistFunc(obj1:SpatialObject,obj2:SpatialObject):Double = obj1.coord.centreDistTo(obj2.coord)
/**
  * 
  */
given defaultDistFunc:((SpatialObject,SpatialObject)=>Double)  = centreDistFunc

/**
  * A spatial rectangle used to describe the boundary range of a spatial object.
  *
  * @param min
  * @param max
  */
case class Rectangle(min:Array[Double],max:Array[Double]):
    private[platdb] def size:Int = 16*dimension
    /**
      * 
      *
      * @return
      */
    def dimension:Int = if min.length <= max.length then min.length else max.length
    /**
      * whether current rectangle is a point.
      *
      * @return
      */
    def isPoint:Boolean = 
        var flag = true
        breakable(
            for i <- 0 until dimension do
                if max(i)!=0.0 && min(i) != max(i) then
                    flag = false
                    break()
        )
        flag
    /**
      * 
      *
      * @return
      */
    def area:Double =
        if isPoint then return 0.0
        var a = 0.0
        for i <- 0 until dimension do
            a*= (max(i)-min(i)) 
        a
    /**
      * 
      *
      * @param r
      * @return
      */
    def intersect(r:Rectangle):Boolean = 
        if dimension!=r.dimension then
            return false
        dimension match
            case 0 => true
            case 1 => !(min(0) > r.max(0) || max(0) < r.min(0))
            case 2 => Min(min(0),r.min(0)) <= Min(max(0),r.max(0)) && Max(min(1),r.min(1)) <= Min(max(1),r.max(1))
            case n =>
                var flag = true
                breakable(
                    for i <- 1 to n do
                        if !this.projectFollow(i).intersect(r.projectFollow(i)) then
                            flag = false
                            break()
                )
                flag 
    /**
      * Project along the axis, projecting the current rectangle onto a plane other than that axis.
      * for example,(x,y,z) -> (y,z), axis=1
      * 
      * @param axis
      * @return
      */
    private def projectFollow(axis:Int):Rectangle = 
        if axis <= 0 || axis>dimension then
            throw new Exception(s"project axis($axis) out of bound [1,${dimension}]")
        dimension match
            case 0 => this
            case 1 => Rectangle(Array[Double](0),Array[Double](0))
            case n =>  
                val a = (for i <- 0 until dimension if i!=n-1 yield min(i)).toArray
                val b = (for i <- 0 until dimension if i!=n-1 yield max(i)).toArray
                Rectangle(a,b)
    /**
      * whether current rectangle can corver r.
      *
      * @param r
      * @return
      */
    def cover(r:Rectangle):Boolean =
        if dimension != r.dimension then
            return false
        var cov = true
        breakable(
            for i <- 0 until dimension do
                if min(i)>=r.min(i) || max(i)<=r.max(i) then
                    cov = false
                    break()
        )
        cov
    /**
      * whether current rectangle be corverd by r.
      *
      * @param r
      * @return
      */
    def beCoverd(r:Rectangle):Boolean = r.cover(this)
    /**
      * 要覆盖r所需要的最小面积扩张
      *
      * @param r
      * @return
      */
    def enlargeAreaToCover(r:Rectangle):(Double,Rectangle) = 
        val x = Array[Double](dimension)
        val y = Array[Double](dimension)
        var cover = true
        for i <- 0 until dimension do
            if min(i) <= r.min(i) then
                x(i) = min(i)
            else 
                x(i) = r.min(i)
                cover = false
            if max(i) >= r.max(i) then
                y(i) = max(i)
            else 
                y(i) = r.max(i)
                cover = false
        if cover then
            (0.0,this)
        else 
            val cov = Rectangle(x,y)
            (cov.area - area,cov)
    /**
      * 
      *
      * @param r
      * @return
      */
    def areaDifference(r:Rectangle):Double = area - r.area
    /**
      * 
      *
      * @param r
      * @return
      */
    def -(r:Rectangle):Double = area - r.area
    /**
      * 
      *
      * @param r
      * @return
      */
    def >=(r:Rectangle):Boolean = area >= r.area
    /**
      * 
      *
      * @param p
      * @return
      */
    private[platdb] def centreDistToPoint(p:Array[Double]):Double = 
        var d:Double = 0.0
        for i <- 0 until dimension do
            val n = (max(i)+min(i))/2 -p(i)
            d+=n*n
        d
    /**
      * 
      *
      * @param b
      * @return
      */
    private[platdb] def centreDistTo(b:Rectangle):Double = 
        var d:Double = 0.0
        for i <- 0 until dimension do
            val n = (max(i)+min(i)-b.max(i)-b.min(i))/2 
            d+=n*n
        d
    private def Max(a:Double,b:Double):Double = if a>b then a else b
    private def Min(a:Double,b:Double):Double = if a<b then a else b

/**
  * 
  *
  * @param coord
  * @param key
  * @param data
  * @param isPoint
  */
case class SpatialObject(coord:Rectangle,key:String,data:String,isPoint:Boolean):
    def dimension:Int = coord.dimension

// meta info of region.
private[platdb] class RegionValue(var root:Long,val dimension:Int,val maxEntries:Int,val minEntries:Int):
    def getBytes:Array[Byte] = 
        var a = (root >> 32).toInt
        var b = (root & 0x00000000ffffffffL).toInt
        var c = dimension
        var d = maxEntries
        var e = minEntries
        var arr = new Array[Byte](RTreeBucket.metaSize)
        for i <- 0 to 3 do
            arr(3-i) = ( a & 0xff).toByte
            arr(7-i) = ( b & 0xff).toByte
            arr(11-i) = ( c & 0xff).toByte
            arr(15-i)= ( d & 0xff).toByte
            arr(19-i)= ( e & 0xff).toByte
            a = a >> 8
            b = b >> 8
            c = c >> 8
            d = d >> 8
            e = e >> 8
        arr
    //
    override def toString():String = Base64.getEncoder().encodeToString(getBytes)

// for leaf node child is -1, for branch node key is null.
private[platdb] class Entry(var mbr:Rectangle,var child:Long,var key:String):
    def size:Int = mbr.size + key.getBytes.length + 8
    def keySize:Int = key.getBytes.length
    def getKeyBytes:Array[Byte] = key.getBytes


extension (arr:ArrayBuffer[Entry])
    def mbr:Rectangle = 
        if arr.length == 0 then
            Rectangle(Array[Double](0),Array[Double](0))
        else
            val d = arr(0).mbr.dimension
            var x = Array[Double](d)
            var y = Array[Double](d)
            for i <- 0 until d do
                var m = Double.MaxValue
                var n = Double.MinValue
                for e <- arr do
                    if e.mbr.min(i) < m then
                        m = e.mbr.min(i)
                    if e.mbr.max(i) > n then
                        n = e.mbr.max(i)
                x(i) = m
                y(i) = n
            Rectangle(x,y)
    def size:Int = 
        var sz = BlockHeader.size
        if arr.length > 0 then
            val d = arr(0).mbr.dimension
            sz+= RNode.entryIndexSize(d)*arr.length
            for e <- arr do
                sz += e.keySize
        sz

// for branch node--> offset<<32 | keySize == child.
private[platdb] case class EntryIndex(mbr:Rectangle,offset:Int,keySize:Int)

private[platdb] object RNode:
    def entryIndexSize(dimension:Int):Int = 16*dimension+8
    //
    def entries(blk:Block,dimension:Int):Try[ArrayBuffer[Entry]] = 
        blk.getBytes() match
            case None => Failure(new Exception(s"null blcok ${blk.id},cannot get entries from it"))
            case Some(data) =>
                val idxSize = entryIndexSize(dimension)
                if data.length < blk.header.count*idxSize then
                    return Failure(new Exception(s"block ${blk.id} data format wrong"))
                var entries = new ArrayBuffer[Entry]()
                var err:Boolean = false 
                breakable( 
                    for i <- 0 until blk.header.count do
                        val idx = data.slice(idxSize*i,(i+1)*idxSize) 
                        unmashalIndex(idx,dimension) match
                            case None => 
                                err = true
                                break() 
                            case Some(ei) =>
                                var child = -1L
                                var key:String = ""
                                if blk.header.flag == leafType then
                                    val off = ei.offset - BlockHeader.size
                                    key = new String(data.slice(off,off+ei.keySize))
                                else 
                                    child = (ei.offset & 0x00000000ffffffffL) << 32 | (ei.keySize & 0x00000000ffffffffL)
                                entries+=new Entry(ei.mbr,child,key)
                )
                if !err then Success(entries) else Failure(new Exception("parse block data failed"))
    //
    def apply(blk:Block,dimension:Int):Try[RNode] = 
        var node:Option[RNode] = None
        entries(blk,dimension) match
            case Failure(e) => Failure(e)
            case Success(es) =>
                var n = new RNode(blk.header)
                n.entries = es 
                Success(n)
    //
    def marshalIndex(idx:EntryIndex):Array[Byte] = 
        var buf:ByteBuffer = ByteBuffer.allocate(entryIndexSize(idx.mbr.dimension))
        for n <- idx.mbr.min do  
            buf.putDouble(n)
        for n <- idx.mbr.max do 
            buf.putDouble(n)
        buf.putInt(idx.offset)
        buf.putInt(idx.keySize)
        buf.array()
    //
    def unmashalIndex(d:Array[Byte],dimension:Int):Option[EntryIndex] = 
        val off = 16*dimension
        if d.length != off+8 then
            return None
        val a = for i <- Range(0,off/2,8) yield ByteBuffer.wrap(d.slice(8*i,8*i+8).toArray).getDouble()
        val offset =  (d(off) &0xff) << 24 | (d(off+1) & 0xff) << 16 | (d(off+2) & 0xff) << 8 | (d(off+3) & 0xff)
        val keySize = (d(off+4) &0xff) << 24 | (d(off+5) & 0xff) << 16 | (d(off+6) & 0xff) << 8 | (d(off+7) & 0xff)
        Some(EntryIndex(Rectangle(a.slice(0,dimension).toArray,a.slice(dimension+1,a.length).toArray),offset,keySize))

/**
  * 
  *
  * @param header
  */
private[platdb] class RNode(var header:BlockHeader) extends Persistence:
    var unbalanced:Boolean = false
    var spilled:Boolean = false
    var parent:Option[RNode] = None
    var children:ArrayBuffer[RNode] = new ArrayBuffer[RNode]()
    var entries:ArrayBuffer[Entry] = null

    def id:Long = header.pgid
    def length:Int = entries.length
    def isLeaf:Boolean = header.flag == leafType
    def isBranch:Boolean = header.flag == branchType
    def isRoot:Boolean = parent match
        case Some(n) => false
        case None => true
    def root:RNode = parent match {
            case Some(node) => node.root
            case None => this
        }
    def mbr:Rectangle = entries.mbr
    //
    def put(obj:SpatialObject):Unit = 
        if !isLeaf then return None
        var idx = -1
        breakable(
            for i <- 0 until entries.length do
                if entries(i).key == obj.key then
                    idx = i
                    break()
        )
        if idx >= 0 then 
            entries(idx) = Entry(obj.coord,0,obj.key)
        else 
            entries += Entry(obj.coord,0,obj.key)
    //
    def put(oldId:Long,entry:Entry):Unit = 
        var idx = -1
        breakable(
            for i <- 0 until entries.length do
                if entries(i).child == oldId then
                    idx = i
                    break()
        )
        if idx >= 0 then 
            entries(idx) = entry
        else 
            entries += entry
    // 
    def del(key:String):Unit = 
        var idx = -1
        breakable(
            for i <- 0 until entries.length do
                if entries(i).key == key then
                    idx = i
                    break()
        )
        if idx >=0 then
            entries.remove(idx,1)
    //
    def del(keys:Seq[String]):Unit = 
        var es = ArrayBuffer[Entry]()
        for e <- entries do
            for k <- keys do
                if e.key == k then
                    es += e
        entries = es
    //
    def removeChild(node:RNode):Unit = 
        var idx = -1
        breakable(
            for i <- 0 until children.length do
                if children(i).id == node.id then
                    idx = i
                    break()
        )
        if idx >=0 then
            children.remove(idx,1)
    def updateMbr(child:RNode):Unit = 
        breakable(
            for i <- 0 until entries.length do
                if entries(i).child == child.id then
                    val (_,r) = entries(i).mbr.enlargeAreaToCover(child.mbr)
                    entries(i).mbr = r
                    break()
        )
    //
    def size():Int = entries.size
    //
    def writeTo(blk: Block): Int = 
        if isLeaf then
            blk.header.flag = leafType
        else 
            blk.header.flag = branchType
        blk.header.count = entries.length
        blk.header.size = size()
        blk.header.overflow = (size()+DB.pageSize)/DB.pageSize - 1
        // update data.
        blk.append(blk.header.getBytes())
        if entries.length > 0 then
            val indexSize = RNode.entryIndexSize(entries(0).mbr.dimension)
            var idx = blk.size
            var offset = BlockHeader.size+(entries.length*indexSize)
            for e <- entries do 
                val ei = isLeaf match
                    case true => EntryIndex(e.mbr,offset,e.keySize)
                    case false => EntryIndex(e.mbr,(e.child >> 32).toInt,e.child.toInt)
                blk.write(idx,RNode.marshalIndex(ei))
                blk.write(offset,e.getKeyBytes)
                idx+=indexSize
                offset = blk.size
        blk.size
    //
    def linearSplit(sz:Int,minEntries:Int):(ArrayBuffer[Entry],ArrayBuffer[Entry]) =
        var dimension = 0
        if entries.length > 0 then
            dimension = entries(0).mbr.dimension
        // select two points
        val p1 = new Array[Double](dimension)
        val p2 = new Array[Double](dimension)
        for i <- 0 until dimension do
            var mi = Double.MaxValue
            var ma = Double.MinValue
            for e <- entries do
                if e.mbr.min(i) < mi then
                    mi = e.mbr.min(i)
                if e.mbr.max(i) > ma then
                    ma = e.mbr.max(i)
            p1(i) = mi 
            p2(i) = ma
        // split entries according the distance to p1 and p2.
        var a = new ArrayBuffer[Entry]()
        var b = new ArrayBuffer[Entry]()
        for e <- entries do
            if a.size >=sz && a.length >= minEntries then
                b+=e
            else
                val d1 = e.mbr.centreDistToPoint(p1)
                val d2 = e.mbr.centreDistToPoint(p2)
                if d1 < d2 then
                    a+=e
                else if d1 > d2 then
                    b+=e
                else
                    // compare area
                    val ar1 = a.mbr.area
                    val ar2 = b.mbr.area
                    if ar1 < ar2 then
                        a+=e
                    else if ar1 > ar2 then
                        b+=e
                    else 
                        // compare count
                        if a.length <= b.length then
                            a+=e
                        else 
                            b+=e
        (a,b)

/**
  * 
  */
private[platdb] object RTreeBucket:
    val metaKey = "region-meta-value-o8h6d4sva3e9"
    val metaSize = 20
    def unmashalValue(value:String):Option[RegionValue] = 
        val d = Base64.getDecoder().decode(value)
        if d.length!=metaSize then
            return None
        val arr = for i <- 0 to 4 yield
            (d(4*i) & 0xff) << 24 | (d(4*i+1) & 0xff) << 16 | (d(4*i+2) & 0xff) << 8 | (d(4*i+3) & 0xff)
        val root = (arr(0) & 0x00000000ffffffffL) << 32 | (arr(1) & 0x00000000ffffffffL)
        Some(RegionValue(root,arr(2),arr(3),arr(4)))
    //
    def getCoordinate(data:String,dimension:Int):Try[Rectangle] = 
        val i = data.indexOf("|")
        if i < 0 then
            Failure(new Exception(s"not found coordinate info from data"))
        else 
            val bs = Base64.getDecoder().decode(data.slice(0,i))
            if bs.length != 16*dimension then
                Failure(new Exception(s"not found coordinate info from data"))
            else 
                val a = for j <- Range(0,bs.length/8,8) yield ByteBuffer.wrap(bs.slice(8*j,8*j+8)).getDouble()
                Success(Rectangle(a.slice(0,dimension).toArray,a.slice(dimension+1,a.length).toArray))
    //
    def getData(data:String):Try[String] = 
        val i = data.indexOf("|")
        if i < 0 then
            Failure(new Exception(s"not found data info"))
        else 
            Success(data.slice(i+1,data.length()))
    //
    def unwarpData(data:String,dimension:Int):Try[(Rectangle,String)] = 
        val i = data.indexOf("|")
        if i < 0 then
            Failure(new Exception(s"not found coordinate info from data"))
        else 
            val (c,d) = data.splitAt(i)
            val bs = Base64.getDecoder().decode(c)
            if bs.length != 16*dimension then
                Failure(new Exception(s"not found coordinate info from data"))
            else 
                val a = for j <- Range(0,bs.length/8,8) yield ByteBuffer.wrap(bs.slice(8*j,8*j+8)).getDouble()
                Success((Rectangle(a.slice(0,dimension).toArray,a.slice(dimension+1,a.length).toArray),d))
    //
    def wrapData(obj:SpatialObject):String = 
        var buf:ByteBuffer = ByteBuffer.allocate(obj.coord.size)
        for i <- 0 until obj.dimension do
            buf.putDouble(obj.coord.min(i))
        for i <- 0 until obj.dimension do
            buf.putDouble(obj.coord.max(i))
        val c = Base64.getEncoder().encodeToString(buf.array())
        c+"|"+obj.data
    //
    def apply(bk:Bucket,tx:Tx):Try[RTreeBucket] = 
        bk.get(metaKey) match
            case Failure(e) => Failure(e)
            case Success(v) =>
                unmashalValue(v) match
                    case None => Failure(new Exception("parse region meta info failed"))
                    case Some(value) => 
                        var rbk = new RTreeBucket(bk,tx)
                        rbk.value = value
                        Success(rbk)
    //
    def apply(bk:Bucket,dimension:Int,tx:Tx):Try[RTreeBucket] = 
        if dimension<=0 then
            return Failure(new Exception(s"dimension should larger than 0"))
        // TODO if is to large?
        var rbk = new RTreeBucket(bk,tx)
        rbk.value = new RegionValue(-1L,dimension,DB.minEntries,DB.maxEntries)
        Success(rbk)

/**
  * 
  *
  * @param bk
  * @param tx
  */
private[platdb] class RTreeBucket(val bk:Bucket,val tx:Tx) extends Region:
    var value:RegionValue = null
    var root:Option[RNode] = None
    // cache nodes about writeable tx. 
    private var nodes:Map[Long,RNode] = Map[Long,RNode]() 
    // record nodes that need to reinseart when delete.
    private var reInsertNodes = List[RNode]()

    def name:String = bk.name
    def length:Long = bk.length
    def dimension:Int = value.dimension
    //
    def iterator:Iterator[SpatialObject] = new RTreeBucketIter(this)
    //
    def +=(obj: SpatialObject): Unit = 
        put(obj) match
            case Failure(e) => throw e
            case Success(_) => None
    //
    def -=(key: String): Unit = 
        delete(key) match
            case Failure(e) => throw e
            case Success(_) => None 
    //
    def update(key: String, obj: SpatialObject): Unit = 
        delete(key) match
            case Failure(e) => 
                if !DB.isNotExists(e) then
                    throw e
            case Success(_) => None
        put(obj) match
            case Failure(e) => throw e
            case Success(_) => None
    //
    def mark(coordinate:Array[Double],key:String,data:String):Try[Unit] =
        val rect = Rectangle(coordinate,coordinate)
        put(SpatialObject(rect,key,data,rect.isPoint))
    //
    def put(obj:SpatialObject):Try[Unit] = 
        if obj.dimension != dimension then
            return Failure(new Exception(s"the dimension(${obj.dimension}) of object is not match region"))
        bk.get(obj.key) match
            case Failure(e) => 
                if !DB.isNotExists(e) then
                    return Failure(e)
            case Success(v) => 
                delete(obj.key) match
                    case Failure(e) => return Failure(e)
                    case Success(_) => None
        var itr = new RTreeBucketIter(this)
        itr.searchInsertNode(obj.coord) match
            case Failure(e) => return Failure(e)
            case Success(_) => None
        itr.node() match
            case Failure(e) => return Failure(e)
            case Success(node) =>
                bk.put(obj.key,RTreeBucket.wrapData(obj)) match
                    case Failure(e) => return Failure(e)
                    case Success(_) =>
                        node.put(obj)
                        // update search path.
                        var rect = obj.coord
                        try
                            for r <- itr.stack.reverse.tail do
                                (r.node,r.block,r.index) match
                                    case (Some(n),_,idx) => 
                                        val (_,rec) = n.entries(idx).mbr.enlargeAreaToCover(rect)
                                        n.entries(idx).mbr = rec
                                        rect = rec
                                    case (None,Some(b),idx) => 
                                        getNodeByBlock(b) match
                                            case Failure(e) => throw e
                                            case Success(n) =>
                                                val (_,rec) = n.entries(idx).mbr.enlargeAreaToCover(rect)
                                                n.entries(idx).mbr = rec
                                                rect = rec
                                    case (None,None,_) => throw new Exception("search path element is null")
                            Success(None)
                        catch
                            case e:Exception => Failure(e)
    //
    def get(key:String):Try[SpatialObject] = 
        bk.get(key) match
            case Failure(e) => Failure(e)
            case Success(v) =>
                RTreeBucket.unwarpData(v,dimension) match
                    case Failure(e) => Failure(e)
                    case Success(d) =>
                        val (r,data) = d
                        Success(SpatialObject(r,key,data,r.isPoint))
    // 
    def search(range:Rectangle,filter:(SpatialObject)=>Boolean):Try[Seq[SpatialObject]] = 
        if range.dimension!=dimension then
            return Failure(new Exception(s"the dimension(${range.dimension}) of query window is not match region"))
        var itr = new RTreeBucketIter(this)
        itr.searchIntersectNode(range) match
            case Failure(e) => Failure(e)
            case Success(ns) =>
                try
                    var res = List[SpatialObject]()
                    for n <- ns do
                        for e <- n.entries do
                            val v = bk(e.key)
                            RTreeBucket.unwarpData(v,dimension) match
                                case Failure(exception) => throw exception
                                case Success((_,data)) =>
                                    val so = SpatialObject(e.mbr,e.key,data,e.mbr.isPoint)
                                    if filter(so) then
                                        res:+=so
                    Success(res)
                catch
                    case e:Exception => Failure(e)
    //
    def scan(filter:(SpatialObject)=>Boolean):Try[Seq[SpatialObject]] = 
        try
            Success((for so <- iterator if filter(so) yield so).toList)
        catch
            case e:Exception => Failure(e)
    //
    def delete(key:String):Try[Unit] = 
        bk.get(key) match
            case Failure(e) => Failure(e)
            case Success(v) =>
                RTreeBucket.unwarpData(v,dimension) match
                    case Failure(exception) => throw exception
                    case Success((r,_)) => delete(r,(s:SpatialObject) => s.key == key)
    //
    def delete(range:Rectangle,filter:(SpatialObject)=>Boolean):Try[Unit] = 
        var itr = new RTreeBucketIter(this)
        itr.searchIntersectNode(range) match
            case Failure(e) => return Failure(e)
            case Success(ns) =>
                try
                    for n <- ns do
                        var keys = List[String]()
                        for e <- n.entries do
                            val v = bk(e.key) // TODO ignore not exists err
                            RTreeBucket.unwarpData(v,dimension) match
                                case Failure(exception) => throw exception
                                case Success((_,data)) =>
                                    val so = SpatialObject(e.mbr,e.key,data,e.mbr.isPoint)
                                    if filter(so) then
                                        keys:+=e.key
                        for k <- keys do
                            bk.delete(k) match
                                case Failure(e) => throw e // TODO ignore not exists err
                                case Success(_) => None
                            n.del(keys)
                    Success(None)
                catch
                    case e:Exception => Failure(e)
    //
    def delete(range:Rectangle):Try[Unit] =
        var itr = new RTreeBucketIter(this)
        itr.searchCoverNode(range) match
            case Failure(e) => return Failure(e)
            case Success(ns) =>
                try
                    for n <- ns do
                        var keys = List[String]()
                        for e <- n.entries do
                            if e.mbr.cover(range) then
                                keys :+= e.key 
                        for k <- keys do
                            bk.delete(k) match
                                case Failure(e) => throw e // TODO ignore not exists err
                                case Success(_) => None
                            n.del(keys)
                    Success(None)
                catch
                    case e:Exception => Failure(e)
    //
    def boundary():Try[Rectangle] = 
        root match
            case Some(node) => Success(node.mbr)
            case None => 
                getNode(value.root) match
                    case Failure(e) => Failure(e)
                    case Success(node) => Success(node.mbr)
    //
    def nearby(obj:SpatialObject,filter:(SpatialObject)=>Boolean)(using distFunc:(SpatialObject,SpatialObject)=>Double):Try[Seq[(SpatialObject,Double)]] =
        var list = List[(SpatialObject,Double)]()
        var que = new queue()
        root match
            case None => getNode(value.root) match
                case Failure(e) => return Failure(e)
                case Success(node) =>
                    root = Some(node)
                    que.push(element(0.0,node.mbr,Some(node),None))
            case Some(node) => que.push(element(0.0,node.mbr,Some(node),None))
        var err:Option[String] = None
        breakable(
            while true do
                que.pop() match
                    case None => break()
                    case Some(elem) =>
                        elem.node match
                            case None =>
                                elem.obj match 
                                    case None => None
                                    case Some(ob) => 
                                        if filter(ob) then
                                           list:+=(ob,elem.dist) 
                            case Some(node) =>
                                for e <- node.entries do
                                    val sobj =  SpatialObject(e.mbr,e.key,"",e.mbr.isPoint)
                                    var elt:element = null
                                    if node.isLeaf then
                                        elt = element(distFunc(obj,sobj),e.mbr,None,Some(sobj))
                                    else 
                                        getNode(e.child) match
                                            case Failure(e) => 
                                                err = Some(e.getMessage())
                                                break()
                                            case Success(n) =>
                                                elt = element(distFunc(obj,sobj),e.mbr,Some(n),None)
                                    que.push(elt)       
        )
        err match
            case Some(msg) => Failure(new Exception(msg))
            case None => Success(list)
    //
    def nearby(obj:SpatialObject,k:Int)(using distFunc:(SpatialObject,SpatialObject)=>Double):Try[Seq[(SpatialObject,Double)]] = 
        if k<=0 then
            return Failure(new Exception("parameter k should larger than zero"))
        var list = List[(SpatialObject,Double)]()
        var que = new queue()
        root match
            case None => getNode(value.root) match
                case Failure(e) => return Failure(e)
                case Success(node) =>
                    root = Some(node)
                    que.push(element(0.0,node.mbr,Some(node),None))
            case Some(node) => que.push(element(0.0,node.mbr,Some(node),None))
        var err:Option[String] = None
        breakable(
            while true do
                que.pop() match
                    case None => break()
                    case Some(elem) =>
                        elem.node match
                            case None =>
                                elem.obj match 
                                    case None => None
                                    case Some(ob) => 
                                        if list.length < k then
                                            list:+=(ob,elem.dist) 
                                        if list.length >= k then
                                            break()
                            case Some(node) =>
                                for e <- node.entries do
                                    val sobj = SpatialObject(e.mbr,e.key,"",e.mbr.isPoint)
                                    var elt:element = null
                                    if node.isLeaf then
                                        elt = element(distFunc(obj,sobj),e.mbr,None,Some(sobj))
                                    else 
                                        getNode(e.child) match
                                            case Failure(e) => 
                                                err = Some(e.getMessage())
                                                break()
                                            case Success(n) =>
                                                elt = element(distFunc(obj,sobj),e.mbr,Some(n),None)
                                    que.push(elt)       
        )
        err match
            case Some(msg) => Failure(new Exception(msg))
            case None => Success(list)
    //
    def nearby(key:String,k:Int)(using distFunc:(SpatialObject,SpatialObject)=>Double):Try[Seq[(SpatialObject,Double)]] = 
        get(key) match
            case Failure(e) => Failure(e)
            case Success(obj) => nearby(obj,k)(using distFunc)
    //
    def nearby(obj:SpatialObject,d:Double,limit:Int)(using distFunc:(SpatialObject,SpatialObject)=>Double):Try[Seq[(SpatialObject,Double)]] = 
        if limit <= 0 then
            return Failure(new Exception("parameter limit should larger than zero"))
        if d < 0.0 then
            return Failure(new Exception("parameter d should larger than zero"))
        var list = List[(SpatialObject,Double)]()
        var que = new queue()
        root match
            case None => getNode(value.root) match
                case Failure(e) => return Failure(e)
                case Success(node) =>
                    root = Some(node)
                    que.push(element(0.0,node.mbr,Some(node),None))
            case Some(node) => que.push(element(0.0,node.mbr,Some(node),None))
        var err:Option[String] = None
        breakable(
            while true do
                que.pop() match
                    case None => break()
                    case Some(elem) =>
                        elem.node match
                            case None =>
                                elem.obj match 
                                    case None => None
                                    case Some(ob) => 
                                        if list.length < limit then
                                            list:+=(ob,elem.dist) 
                                        if list.length >= limit then
                                            break()
                            case Some(node) =>
                                for e <- node.entries do
                                    val sobj =  SpatialObject(e.mbr,e.key,"",e.mbr.isPoint)
                                    if node.isLeaf then
                                        val elt = element(distFunc(obj,sobj),e.mbr,None,Some(sobj))
                                        if elt.dist <= d then
                                            que.push(elt)
                                    else
                                        getNode(e.child) match
                                            case Failure(e) => 
                                                err = Some(e.getMessage())
                                                break()
                                            case Success(n) =>
                                                que.push(element(distFunc(obj,sobj),e.mbr,Some(n),None))                   
        )
        err match
            case Some(msg) => Failure(new Exception(msg))
            case None => Success(list)
    //
    def getData(key:String):Try[String] = 
        try 
            val v = bk(key)
            RTreeBucket.unwarpData(v,dimension) match
                case Failure(exception) => throw exception
                case Success((_,data)) => Success(data)
        catch
            case e:Exception => Failure(e)
    //
    def nodeOrBlock(id:Long):Try[(Option[RNode],Option[Block])] = 
        if nodes.contains(id) then 
            Success((nodes.get(id),None))
        else 
            if id < 0 then
                // if id <0 means the bucket is a new created in memory, not loaded from disk.
                return Success((root,None))
            tx.block(id) match
                case Success(bk) => Success((None,Some(bk)))
                case Failure(e) => Failure(e)
    //
    def nodeEntries(block:Option[Block]):Try[ArrayBuffer[Entry]] = 
        block match
            case None => Failure(new Exception("block is null,cannot get entries from it")) 
            case Some(blk) => 
                if nodes.contains(blk.id) then
                    Success(nodes(blk.id).entries)
                else
                    RNode.entries(blk,dimension)
    //
    def getNodeByBlock(block:Block):Try[RNode] = 
        if nodes.contains(block.id) then 
            Success(nodes(block.id))
        else
            RNode(block,dimension) match
                case Failure(e) => Failure(e)
                case Success(node) => 
                    nodes(node.id) = node
                    Success(node)
    //
    def getNodeChild(node:Option[RNode],idx:Int):Try[RNode] = 
        node match
            case None => Failure(new Exception("node is null,cannot get its child"))
            case Some(n) =>
                if n.isLeaf || idx<0 || idx>=n.length then
                    Failure(new Exception("node is leaf or index out of bound"))
                else
                    getNode(n.entries(idx).child) match
                        case Failure(e) => Failure(e)
                        case Success(child) =>
                            child.parent = Some(n)
                            n.children += child
                            Success(child)
    //
    def getNode(id:Long):Try[RNode] =
        if nodes.contains(id) then 
            Success(nodes(id))
        else
            tx.block(id) match
                case Failure(e) => Failure(e)
                case Success(block) => getNodeByBlock(block)
    //
    def merge():Unit =
        reInsertNodes = List[RNode]()
        for (_,node) <- nodes do
            if node.isLeaf then
                condense(node,DB.pageSize / 4)
        for node <- reInsertNodes do
            reinstert(node)
        for node <- reInsertNodes do
            freeNode(node)
    //
    def split():Unit = 
        root match
            case None => None
            case Some(node) => splitOnNode(node)
        root match
            case None => None
            case Some(node) =>
                var r = node.root
                if r.id >= tx.maxPageId then
                    throw new Exception(s"pgid ${r.id} above high water mark ${tx.maxPageId}")
                value.root = r.id 
                root = Some(r)
                bk+=(RTreeBucket.metaKey,value.toString())
    //
    private def splitOnNode(node:RNode):Unit = 
        if node.spilled then return None
        //
        val n = node.children.length
        for i <- 0 until n do 
            splitOnNode(node.children(i))
        
        if node.children.length!=0 then
            node.children = new ArrayBuffer[RNode]()
        
        // split current node.
        for n <- splitNode(node,DB.pageSize) do 
            val oldId = n.id
            if oldId > 0 then 
                tx.free(oldId)
                n.header.pgid = 0
            
            // allocate a new block and write node content to it.
            val nid = tx.allocate(n.size()) 
            if nid >= tx.maxPageId then 
                throw new Exception(s"pgid $nid above high water mark ${tx.maxPageId}")
            var blk = tx.makeBlock(nid,n.size())
            n.header.pgid = blk.id
            n.writeTo(blk)
            n.spilled = true
            // insert the new node info to its parent.
            n.parent match
                case None => None
                case Some(p) => p.put(oldId,Entry(n.mbr,n.id,""))
        
        // if the old root was splitd and created a new one, we need split it as well.
        node.parent match
            case None => None
            case Some(p) => 
                if p.id <= 0 then
                    node.children = new ArrayBuffer[RNode]()
                    splitOnNode(p)
    //   
    private def splitNode(node:RNode,sz:Int):List[RNode] = 
        cutNode(node,sz) match
            case (head,None) => List(head)
            case (head,Some(tail)) => List(head):::splitNode(tail,sz)
    //
    private def cutNode(node:RNode,sz:Int):(RNode,Option[RNode]) =
        if node.size() > sz && node.length > value.maxEntries then
            val threshold = (sz*DB.fillPercent).toInt
            val (es1,es2) = node.linearSplit(threshold,value.minEntries)
            node.entries = es1
            //
            var nodeB = new RNode(new BlockHeader(-1L,node.header.flag,0,0,0))
            nodeB.entries = es2
            // if current node's parent is null, then create a new one.
            node.parent match
                case Some(p) => 
                    nodeB.parent = Some(p)
                    p.children+=nodeB
                case None =>
                    var parent = new RNode(new BlockHeader(-1L,branchType,0,0,0))
                    parent.children+=node
                    parent.children+=nodeB
                    node.parent = Some(parent)
                    nodeB.parent = Some(parent)
            (node,Some(nodeB))
        else
            (node,None)
    //
    private def insert(entry:Entry):Unit = 
        var itr = new RTreeBucketIter(this)
        itr.searchInsertNode(entry.mbr) match
            case Failure(e) => throw e
            case Success(_) => None
        itr.node() match
            case Failure(e) => throw e
            case Success(node) => node.put(-1,entry)
    //
    private def reinstert(node:RNode):Unit = 
        if node.isLeaf then
            for e <- node.entries do
                insert(e)
        else
            for e <- node.entries do
                getNode(e.child) match
                    case Failure(e) => throw e
                    case Success(n) => reinstert(n)
    //
    private def condense(node:RNode,threshold:Int):Unit = 
        if !node.unbalanced then return None
        node.unbalanced = false
        node.parent match
            case None => 
                // current node is root.
                if !node.isLeaf && node.length == 1 then 
                    getNodeChild(Some(node),0) match
                        case Failure(e) => throw new Exception(s"merge root node failed,query child error ${e.getMessage()}")
                        case Success(child) => 
                            child.parent = None // set the child as new root and release old root
                            value.root = child.id
                            root = Some(child)
                            node.removeChild(child)
                            nodes.remove(node.id)
                            freeNode(node)
            case Some(p) =>
                // check whether the node meets the threshold.
                if node.size() > threshold && node.length >= value.minEntries then
                    // adjust mbr of parent.
                    p.updateMbr(node)
                else 
                    // remove entry from parent.
                    p.removeChild(node)
                    // record the node(will reinsert its entries after merge).
                    reInsertNodes :+= node
                condense(p,threshold)
    //
    def clear():Try[Unit] = 
        try
            root match
                case None => 
                    getNode(value.root) match
                        case Failure(e) => throw e
                        case Success(node) => freeNode(node) 
                case Some(node) => freeNode(node)
            Success(None)
        catch
            case e:Exception => Failure(e)
        
    // release subtree or node.
    private def freeNode(node:RNode):Unit = 
        if node.isLeaf then
            if node.id > DB.meta1Page then
                tx.free(node.id)
                node.header.pgid = 0
                nodes.remove(node.id)
        else
            for e <- node.entries do
                getNode(e.child) match
                    case Success(n) => freeNode(n)
                    case Failure(exception) => throw exception
            if node.id > DB.meta1Page then
                tx.free(node.id)
                node.header.pgid = 0
                nodes.remove(node.id)
//
private[platdb] case class element(dist:Double,rect:Rectangle,node:Option[RNode],obj:Option[SpatialObject])
//
private[platdb] class queue:
    private var nodes = new ArrayBuffer[element]()
    def length:Int = nodes.length
    //
    def push(elem:element):Unit  = 
        nodes+=elem
        // shiftUp
        var i = nodes.length-1
        breakable(
            while i > 0 do
                val p = (i-1)/2
                if nodes(p).dist > nodes(i).dist then 
                    val tmp = nodes(i)
                    nodes(i) = nodes(p)
                    nodes(p) = tmp
                    i = p
                else
                    break()
        )
    //
    def pop():Option[element] = 
        if nodes.length == 0 then
            return None
        val e = nodes(0)
        nodes(0) = nodes(nodes.length-1)
        nodes.dropRight(1)
        // shiftDown
        var i = 0
        breakable(
            while i < nodes.length do
                val p1 = i*2 + 1
                val p2 = i*2+2
                if p1 < nodes.length && p2 < nodes.length then
                    if nodes(i).dist <= nodes(p1).dist && nodes(i).dist <= nodes(p2).dist then 
                        break()
                    else
                        val p = if nodes(p1).dist < nodes(p2).dist then p1 else p2
                        val tmp = nodes(i)
                        nodes(i) = nodes(p)
                        nodes(p) = tmp
                        i = p
                else if p1 < nodes.length then 
                    if nodes(i).dist <= nodes(p1).dist then
                        break()
                    else
                        val tmp = nodes(i)
                        nodes(i) = nodes(p1)
                        nodes(p1) = tmp
                        i = p1
                else
                    break()
        )
        Some(e)
//
private[platdb] class RRecord(var node:Option[RNode],var block:Option[Block],var index:Int):
    def isLeaf:Boolean =
        (node,block) match
            case (None,None) => false
            case (Some(n),_) => n.isLeaf
            case (_,Some(b)) => b.header.flag == leafType
    def isBranch:Boolean =
        (node,block) match
            case (None,None) => false
            case (Some(n),_) => n.isBranch
            case (_,Some(b)) => b.header.flag == branchType

//
private[platdb] class RTreeBucketIter(val rbk:RTreeBucket) extends Iterator[SpatialObject]:
    var stack = List[RRecord]()

     def hasNext():Boolean = ???
     def next():SpatialObject = ???

    // 查询所有可能完全覆盖rect的节点
    def searchCoverNode(rect:Rectangle):Try[Seq[RNode]] = 
        rbk.getNode(rbk.value.root) match
            case Failure(e) => Failure(e)
            case Success(n) =>
                rbk.root match
                    case None => rbk.root = Some(n)
                    case Some(_) => None
                try
                    val res = seekCoverNode(rect,n)
                    Success(res)
                catch
                    case e:Exception => Failure(e)    
    private def seekCoverNode(rect:Rectangle,node:RNode):List[RNode] =
        if node.isLeaf then
            List[RNode](node)
        else
            var res = List[RNode]()
            for i <- 0 until node.entries.length do
                if node.entries(i).mbr.cover(rect) then
                    rbk.getNodeChild(Some(node),i) match
                        case Failure(e) => throw e
                        case Success(cn) => res:++= seekCoverNode(rect,cn)
            res
    // 搜索所有和给定rect相交的对象
    def searchIntersectNode(rect:Rectangle):Try[Seq[RNode]]  =
        rbk.getNode(rbk.value.root) match
            case Failure(e) => Failure(e)
            case Success(n) =>
                rbk.root match
                    case None => rbk.root = Some(n)
                    case Some(_) => None
                try
                    val res = seekIntersectNode(rect,n)
                    Success(res)
                catch
                    case e:Exception => Failure(e) 
    private def seekIntersectNode(rect:Rectangle,node:RNode):List[RNode] = 
        if node.isLeaf then
            List[RNode](node)
        else
            var res = List[RNode]()
            for i <- 0 until node.entries.length do
                if node.entries(i).mbr.intersect(rect) then
                    rbk.getNodeChild(Some(node),i) match
                        case Failure(e) => throw e
                        case Success(cn) => res:++= seekIntersectNode(rect,cn)
            res

    // 搜索插入节点
    def searchInsertNode(rect:Rectangle):Try[Unit] =
        try
            seek(rect,rbk.value.root)
            Success(None)
        catch
            case e:Exception => Failure(e)
    def seek(rect:Rectangle,id:Long):Unit = 
        var r:RRecord = null
        rbk.nodeOrBlock(id) match
            case Failure(e) => throw e
            case Success(nb) =>
                nb match
                    case (None,None) => throw new Exception(s"not found node or block for id:$id")
                    case (Some(n),_) => r = new RRecord(Some(n),None,0)
                    case (_,Some(b)) =>
                        if b.btype!= branchType && b.btype!=leafType then 
                            throw new Exception(s"page ${id} invalid page type:${b.btype}")
                        r = new RRecord(None,Some(b),0)
        stack:+=r
        if !r.isLeaf then 
            r.node match
                case Some(node) => seekOnNode(rect,node)
                case None => seekOnBlock(rect,r.block) 
    def seekOnBlock(rect:Rectangle,block:Option[Block]):Unit = 
        rbk.nodeEntries(block) match
            case Failure(e) => throw e
            case Success(entries) =>
                var idx = -1
                var area = Double.MaxValue
                for i <- 0 until entries.length do
                    val (ar,er) = entries(i).mbr.enlargeAreaToCover(rect)
                    if ar < area then
                        idx = i
                        area = ar
                    else if ar == area && idx>=0 then // TODO Double value compare
                        if entries(idx).mbr>=entries(i).mbr then
                            idx = i
                if idx < 0 then
                    throw new Exception("search on a empty block")
                var r= stack.last
                r.index = idx 
                stack = stack.init
                stack :+= r
                seek(rect,entries(idx).child)
    def seekOnNode(rect:Rectangle,node:RNode):Unit = 
        var idx = -1
        var area = Double.MaxValue
        for i <- 0 until node.entries.length do
            val (ar,er) = node.entries(i).mbr.enlargeAreaToCover(rect)
            if ar < area then
                idx = i
                area = ar
            else if ar == area && idx>=0 then // TODO Double value compare
                if node.entries(idx).mbr>=node.entries(i).mbr then
                    idx = i
        if idx < 0 then
            throw new Exception(s"search on a empty node ${node.id}")
        var r = stack.last
        r.index = idx 
        stack = stack.init
        stack :+= r
        seek(rect,node.entries(idx).child)
    def node():Try[RNode] = 
        if stack.length == 0 then 
            return Failure(new Exception("search path is null"))
        var r = stack.last
        r.node match 
            case Some(node) =>
                if r.isLeaf then
                    return Success(node) 
            case None => None
        
        var n:Option[RNode] = None
        r = stack.head
        (r.node,r.block) match
            case (None,None) => return Failure(new Exception("search path is null"))
            case (Some(nd),_) => n = Some(nd)
            case (_,Some(bk)) =>
                rbk.getNodeByBlock(bk) match
                    case Failure(e) => return Failure(e)
                    case Success(n) =>
                        r.node = Some(n)
                rbk.root match
                    case None => rbk.root = r.node
                    case Some(_) => None
                n = r.node
        // top-down: convert blocks on search path to nodes.
        try
            for r <- stack.init do 
                rbk.getNodeChild(n,r.index) match
                    case Success(nd) => n = Some(nd) 
                    case Failure(e) => throw e
        catch
            case e:Exception => return Failure(e)
        n match
            case None => Failure(new Exception("not found leaf node"))
            case Some(node) =>
                if !node.isLeaf then 
                    Failure(new Exception("not found leaf node"))
                else 
                    Success(node)
