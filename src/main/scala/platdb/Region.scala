package platdb

import scala.collection.mutable.{ArrayBuffer,Map}
import scala.util.{Try,Failure,Success}
import scala.util.control.Breaks._


/**
  * 
  *
  * @param min
  * @param max
  */
case class Rectangle(min:Array[Double],max:Array[Double]):
    private[platdb] def size:Int = 16*dimension
    def dimension:Int = if min.length <= max.length then min.length else max.length
    // whether current rectangle is a point.
    def isPoint:Boolean = 
        var flag = true
        breakable(
            for i <- 0 until dimension do
                if max(i)!=0.0 && min(i) != max(i) then
                    flag = false
                    break()
        )
        flag
    // 计算当前矩形和r重叠部分的面积
    def intersect(r:Rectangle):Boolean = ???
    // whether current rectangle can corver r.
    def cover(r:Rectangle):Boolean = ???
    // whether current rectangle be corverd by r.
    def beCoverd(r:Rectangle):Boolean = ???
    // calculate the reatangle area.
    def area:Double = ???
    // 计算当前举行最小需要增加多少面积才能完全覆盖r
    def areaEnlargementForCover(r:Rectangle):Double = ???
    // 扩张当前矩形以便覆盖r
    def enlarge(r:Rectangle):Rectangle = ??? 
    // 计算面积差
    def areaDifference(r:Rectangle):Double = ???
    // 计算面积差
    def -(r:Rectangle):Double = ???
    // 面积比较
    def >=(r:Rectangle):Boolean = ??? 

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

// region的元信息
private[platdb] class RegionValue(var root:Long,val dimension:Int,val maxEntries:Int,val minEntries:Int):
    def getBytes:Array[Byte] = ???
    override def toString():String = ???

// rtree 节点元素，叶节点child为-1，value为数据对象对应的key，分支节点的value字段为null
private[platdb] class Entry(var mbr:Rectangle,var child:Long,var key:String):
    def keySize:Int = ???
    def getKeyBytes:Array[Byte] = ???

// for branch node--> offset<<32 | keySize == child
private[platdb] case class EntryIndex(mbr:Rectangle,offset:Int,keySize:Int)

private[platdb] object RNode:
    def entryIndexSize(dimension:Int):Int = 16*dimension+8

    def entries(blk:Block):Option[ArrayBuffer[Entry]] = ???
    def apply(blk:Block):Option[RNode] = ???
    def marshalIndex(idx:EntryIndex):Array[Byte] = ???  
    def unmashalIndex(d:Array[Byte]):Option[EntryIndex] = ???

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
    def mbr:Rectangle = ???
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
                    entries(i).mbr = entries(i).mbr.enlarge(child.mbr)
                    break()
        )
    def linearSplit(threshold:Int,minEntry:Int):(ArrayBuffer[Entry],ArrayBuffer[Entry]) = ???
    def size():Int = 
        var sz:Int = BlockHeader.size
        if entries.length > 0 then
            val d = entries(0).mbr.dimension
            sz+= RNode.entryIndexSize(d)*entries.length
            for e <- entries do
                sz += e.keySize
        sz
    def writeTo(block: Block): Int = ???

private[platdb] object RTreeBucket:
    val metaKey = "region-meta-value-2333hh"
    val metaSize = 20
    def unmashalValue(d:Array[Byte]):Option[RegionValue] = ???
    def getCoordinate(data:String):Try[Rectangle] = ???
    def getData(data:String):Try[String] = ???
    def unwarpData(v:String):Try[(Rectangle,String)] = ???
    def wrapData(obj:SpatialObject):String = ???
    def apply(bk:Bucket,tx:Tx):Try[RTreeBucket] = 
        bk.get(metaKey) match
            case Failure(e) => Failure(e)
            case Success(v) =>
                unmashalValue(v.getBytes("ascii")) match
                    case None => Failure(new Exception("parse region meta info failed"))
                    case Some(value) => 
                        var rbk = new RTreeBucket(bk,tx)
                        rbk.value = value
                        Success(rbk)
    def apply(bk:Bucket,dimension:Int,tx:Tx):Try[RTreeBucket] = 
        if dimension<=0 then
            return Failure(new Exception(s"dimension should larger than 0"))
        // TODO if is to large?
        var rbk = new RTreeBucket(bk,tx)
        rbk.value = new RegionValue(-1L,dimension,DB.minEntries,DB.maxEntries)
        Success(rbk)

// bk用来存储对象信息，以及Rtree的元信息
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
                                        n.entries(idx).mbr = n.entries(idx).mbr.enlarge(rect)
                                        rect = n.entries(idx).mbr
                                    case (None,Some(b),idx) => 
                                        getNodeByBlock(b) match
                                            case Failure(e) => throw e
                                            case Success(n) =>
                                                n.entries(idx).mbr = n.entries(idx).mbr.enlarge(rect)
                                                rect = n.entries(idx).mbr
                                    case (None,None,_) => throw new Exception("search path element is null")
                            Success(None)
                        catch
                            case e:Exception => Failure(e)
    //
    def get(key:String):Try[SpatialObject] = 
        bk.get(key) match
            case Failure(e) => Failure(e)
            case Success(v) =>
                RTreeBucket.unwarpData(v) match
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
                            RTreeBucket.unwarpData(v) match
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
                RTreeBucket.unwarpData(v) match
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
                            RTreeBucket.unwarpData(v) match
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
    def nearby(obj:SpatialObject,k:Int)(using distFunc:(SpatialObject,SpatialObject)=>Double):Try[Seq[(SpatialObject,Double)]] = ???
    def nearby(key:String,k:Int)(using distFunc:(SpatialObject,SpatialObject)=>Double):Try[Seq[(SpatialObject,Double)]] = ???
    def nearby(obj: SpatialObject, d: Double, limit: Int)(using distFunc: (SpatialObject,SpatialObject) => Double): Try[Seq[(SpatialObject, Double)]] = ???
    //
    def getData(key:String):Try[String] = 
        try 
            val v = bk(key)
            RTreeBucket.unwarpData(v) match
                case Failure(exception) => throw exception
                case Success((_,data)) => Success(data)
        catch
            case e:Exception => Failure(e)

    def nodeOrBlock(id:Long):Try[(Option[RNode],Option[Block])] = ???
    def nodeEntries(block:Option[Block]):Try[ArrayBuffer[Entry]] = ???
    def getNodeByBlock(block:Block):Try[RNode] = ???
    def getNodeChild(node:Option[RNode],idx:Int):Try[RNode] = ???
    def getNode(id:Long):Try[RNode] = ???
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
    def clear():Try[Unit] = ???
    // 释放子树或叶子节点
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
                    val er = entries(i).mbr.enlarge(rect)
                    val ar = er.areaDifference(entries(i).mbr)
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
            val er = node.entries(i).mbr.enlarge(rect)
            val ar = er.areaDifference(node.entries(i).mbr)
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
