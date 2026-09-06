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

import java.util.Base64
import java.nio.ByteBuffer
import scala.collection.mutable.{Map,ArrayBuffer}
import scala.util.control.Breaks._
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import akka.http.javadsl.model.headers.IfNoneMatch
import java.nio.charset.StandardCharsets


trait RawBucket:
    def name:Array[Byte]
    def length:Long
    def iterator:RawIterator 
    /**
      * name
      *
      * @param key
      * @return
      */
    def contains(key:Array[Byte]):Boolean
    /**
      * Retrieve the element
      *
      * @param key
      * @return
      */
    def get(key:Array[Byte]):Option[Array[Byte]]
    /**
      * 
      *
      * @param key
      * @param defalutValue
      * @return
      */
    def getOrElse(key:Array[Byte],defalutValue:Array[Byte]):Array[Byte]
    /**
      * Add or update element.
      *
      * @param key
      * @param value
      * @return
      */
    def put(key:Array[Byte],value:Array[Byte]):Unit
    /**
      * delete element.
      *
      * @param key
      * @return
      */
    def delete(key:Array[Byte]):Unit
    /**
      * Open nested subbuckets.
      *
      * @param name
      * @return
      */
    def getBucket(name:Array[Byte]):Option[RawBucket]
    /**
      * Create a subbucket.
      *
      * @param name
      * @return
      */
    def createBucket(name:Array[Byte]):Option[RawBucket]
    /**
      * Create a subbucket.
      *
      * @param name
      * @return
      */
    def createBucketIfNotExists(name:Array[Byte]):Option[RawBucket]
    /**
      * Delete subbucekt
      *
      * @param name
      * @return
      */
    def deleteBucket(name:Array[Byte]):Unit
    /**
      * A convenient way to retrieve element.
      *
      * @param key
      * @return
      * @throws
      */
    def apply(key:Array[Byte]):Array[Byte]
    /**
      * A convenient way to add or update element.
      *
      * @param key
      * @param value
      * @throws
      */
    def +=(key:Array[Byte],value:Array[Byte]):Unit
    /**
      * A convenient way to add or update elements
      *
      * @param elems
      * @throws
      */
    def +=(elems:Seq[(Array[Byte],Array[Byte])]):Unit
    /**
      * A convenient way to delete an element.
      *
      * @param key
      * @throws
      */
    def -=(key:Array[Byte]):Unit
    /**
      * A convenient way to delete elements.
      *
      * @param keys
      * @throws
      */
    def -=(keys:Seq[Array[Byte]]):Unit
    /**
      * A convenient way to add or update element.
      *
      * @param key
      * @param value
      * @throws
      */
    def update(key:Array[Byte],value:Array[Byte]):Unit 
    /**
      * delete all elements and all sub-buckets.
      *
      * @param key
      * @param value
      * @throws
      */
    def clean():Unit


/**
  * for branch node element, its value filed is null,its falg field is 0;
  * for leaf node element, its child field is -1;
  * if the element is a bucketValue then its flag field is branchType otherwise is 0.
  *
  * @param flag
  * @param child
  * @param key
  * @param value
  */
private class RawNodeElement(var flag:Byte,var child:Long,var key:Array[Byte],var value:Array[Byte]): 
    def keySize:Int = if key != null then key.length else 0
    def valueSize:Int = if value != null then value.length else 0

extension (arr:ArrayBuffer[RawNodeElement])
    def findIndex(func:(RawNodeElement) => Boolean):Int = 
        var idx = -1
        var low = 0
        var high = arr.length
        while low < high do
            val mid = (low+high)/2
            if func(arr(mid)) then
                idx = mid
                high = mid
            else
                low = mid+1
        idx

private class RawNode(var header:BlockHeader) extends Persistence:
    var unbalanced:Boolean = false
    var spilled:Boolean = false
    var minKey:Array[Byte] = null
    var parent:Option[RawNode] = None
    var children:ArrayBuffer[RawNode] = new ArrayBuffer[RawNode]()
    var elements:ArrayBuffer[RawNodeElement] = new ArrayBuffer[RawNodeElement]()

    def id:Long = header.pgid
    def length:Int = elements.length 
    def ntype:Byte = header.flag
    def isLeaf:Boolean = header.flag == Block.typeLeaf
    def isBranch:Boolean = header.flag == Block.typeBranch
    def isRoot:Boolean = parent match
        case Some(n) => false
        case None => true
    def root:RawNode = parent match {
            case Some(node:RawNode) => node.root
            case None => this
        }
    /**
      *  insert a element into node. if the return flag is true,means that oldkey already exists.
      *
      * @param oldKey: old key will be overwrited by newKey
      * @param newKey: new key
      * @param newVal: new value
      * @param flag: node element type 
      * @param child: child node id
      */ 
    def put(oldKey:Array[Byte],newKey:Array[Byte], newVal:Array[Byte],flag:Byte,child:Long):Boolean =
        // 1. find insert location
        var ok:Boolean = false 
        val elem:RawNodeElement = new RawNodeElement(flag,child,newKey,newVal)
        if elements.length == 0 then 
            elements += elem 
        else 
            val idx:Int = elements.findIndex((e:RawNodeElement) => Util.compare(e.key, oldKey) >= 0 )
            if idx >= 0 then
                if elements(idx).key.sameElements(oldKey) then
                    elements(idx) = elem
                    ok = true
                else 
                    elements.insert(idx,elem)
            else 
                elements += elem
        ok
    
    /**
      * delete element from current node. if return true flag,which means the key exists.
      *
      * @param key
      */
    def del(key:Array[Byte]):Boolean =
        val idx = elements.findIndex((e:RawNodeElement) => Util.compare(e.key, key) >= 0)
        if idx >= 0 && elements(idx).key.sameElements(key) then
            elements.remove(idx)
            unbalanced = true
            true 
        else
            false

    /**
      * delete child node from children array.
      *
      * @param node
      */
    def removeChild(node:RawNode):Unit =
        var idx:Int = -1 
        breakable(
            for i <- 0 until children.length do 
                if children(i).id == node.id then 
                    idx = i 
                    break()
        )
        if idx >= 0 then 
            children.remove(idx)

    /**
      * The method returns the index position of the child's node in children array.
      *
      * @param node
      * @return
      */
    def childIndex(node:RawNode):Int =
        var idx:Int = -1
        breakable(
            for i <- 0 until elements.length do 
                if elements(i).child == node.id then 
                    idx = i
                    break()
        )
        idx
    def size():Int = 
        var dataSize:Int = BlockHeader.size+(elements.length*Node.indexSize)
        for e <- elements do dataSize += e.keySize + e.valueSize
        dataSize
    def writeTo(bk:Block):Int =
        bk.header.flag = if isLeaf then Block.typeLeaf else Block.typeBranch
        bk.header.count = elements.length
        bk.header.size = size()
        bk.header.overflow = (size()+DB.pageSize)/DB.pageSize - 1
        // update data.
        bk.append(bk.header.getBytes())
        var idx = bk.size
        var offset = BlockHeader.size+(elements.length*Node.indexSize)
        for e <- elements do 
            val ni = isLeaf match
                case true => NodeIndex(offset,e.keySize,e.valueSize,e.flag)
                case false => NodeIndex(offset,e.keySize,e.child,e.flag)

            bk.write(idx,Node.marshalIndex(ni))
            bk.write(offset,e.key)
            bk.append(e.value)
           
            idx += Node.indexSize
            offset = bk.size
        bk.size

/**
  * bucket trait implement by b+ tree.
  *
  * @param bkname
  * @param tx
  */
private class BTreeRawBucket(val bkname:Array[Byte],var tx:Tx) extends RawBucket:
    var bkv:BucketValue = null
    var root:Option[RawNode] = None
    /** cache nodes about writeable tx. */
    var nodes:Map[Long,RawNode] = Map[Long,RawNode]() 
    /** cache sub-buckets */
    var buckets:Map[String,BTreeRawBucket] = Map[String,BTreeRawBucket]()

    // keys number
    def name:Array[Byte] = bkname 
    def length:Long = bkv.count
    def value:BucketValue = bkv 
    def closed:Boolean = tx == null || tx.closed
    /**
      * return a bucket iterator.
      *
      * @return BucketIterator
      */
    def iterator:RawIterator = new BTreeRawBucketIter(this)
    /**
      * 
      *
      * @param key
      * @param value
      */
    def +=(key:Array[Byte],value:Array[Byte]):Unit = put(key,value) 
    /**
      * 
      *
      * @param elems
      */ 
    def +=(elems:Seq[(Array[Byte],Array[Byte])]):Unit = put(elems)
    /**
      * 
      *
      * @param key
      */
    def -=(key:Array[Byte]):Unit = delete(key) 
    /**
      * 
      *
      * @param keys
      */
    def -=(keys:Seq[Array[Byte]]):Unit = delete(keys)
    /**
      * 
      *
      * @param key
      * @param value
      */
    def update(key:Array[Byte],value:Array[Byte]):Unit = this.+=(key,value)
    /**
      * 
      *
      * @param key
      * @return
      */
    def apply(key:Array[Byte]):Array[Byte] = 
        get(key) match
            case Some(value) => value 
            case None => null
    /**
      * 
      *
      * @param key
      * @return
      */
    def contains(key:Array[Byte]):Boolean = 
        if tx.closed then
            throw DB.exTxClosed
        if key == null || key.length == 0 then 
            return false 
        
        val c = iterator
        c.find(key) match 
            case None => false
            case Some(k,_) => key.sameElements(k)
    /**
      * try to retrieve the value for a key in the bucket.
      * Returns is Failure if the key does not exist or the key is a subbucket name.
      * The returned value is only valid for the life of the transaction.
      * @return value of key
      */
    def get(key:Array[Byte]):Option[Array[Byte]] = 
        if tx.closed then
            throw DB.exTxClosed
        if key == null || key.length == 0 then 
            return None 
        
        val c = new BTreeRawBucketIter(this)
        c.find(key) match
            case None => None 
            case Some(k,v) => if key.sameElements(k) then Some(v) else None 
    /**
      * 
      *
      * @param key
      * @param defalutValue
      * @return
      */
    def getOrElse(key:Array[Byte],defalutValue:Array[Byte]):Array[Byte] = get(key) match
        case None => defalutValue
        case Some(v) => v 
    /**
      * put method insert or update(overwritten) the value for a key in the bucket.
      * Put operation will failed if the key is null or too large, or the value is too large.
      * If the bucket was managed by a readonly transaction, not allow put operation on it.
      * @param key
      * @param value
      * @return success flag
      */
    def put(key:Array[Byte],value:Array[Byte]):Unit =
        if tx.closed then
            throw DB.exTxClosed
        else if !tx.writable then 
            throw DB.exNotAllowOp
        else if key == null || key.length == 0 then
            throw DB.exKeyIsNull
        else if  key.length >= DB.maxKeySize then
            throw DB.exKeyTooLarge
        else if value.length >= DB.maxValueSize then
            throw DB.exValueTooLarge

        val c = new BTreeRawBucketIter(this)
        c.search(key) match 
            case (None,_) => None
            case (Some(k,v),f) =>
                if key.sameElements(k) && f == Node.flagBucket then
                    throw new Exception("the value is subbucket,not allow update it by put method")
        c.node() match 
            case None => throw new Exception(s"not found insert node for key:$key")
            case Some(node) =>
                if !node.put(key,key,value,Block.typeLeaf,0) then
                    bkv.count += 1
                None
    //
    def put(elems:Seq[(Array[Byte],Array[Byte])]):Unit = 
        if tx.closed then
            throw DB.exTxClosed
        else if !tx.writable then 
            throw DB.exNotAllowOp
        else if elems == null || elems.length == 0 then
            return None

        for (k,v) <- elems do
            if k == null || k.length == 0 then
                throw DB.exKeyIsNull
            else if k.length >= DB.maxKeySize then
                throw DB.exKeyTooLarge
            else if v != null && v.length >= DB.maxValueSize then
                throw DB.exValueTooLarge
        
        val c = new BTreeRawBucketIter(this)
        for (key,value) <- elems do
            c.search(key) match 
                case (None,_) => None
                case (Some(k,v),f) =>
                    if key.sameElements(k) && f == Node.flagBucket then
                        throw new Exception("the value is subbucket,not allow update it by put method")
            c.node() match 
                case None => throw new Exception(s"not found insert node for key:$key")
                case Some(node) =>
                    if !node.put(key,key,value,Block.typeLeaf,0) then
                        bkv.count+=1
                    None
    /**
      * try to remove a key from the bucket.
      * delete operation will be ignore if the key does not exist.
      * If the bucket was managed by a readonly transaction, not allow put operation on it.
      * @param key
      * @return success flag
      */
    def delete(key:Array[Byte]):Unit = 
        if tx.closed then
            throw DB.exTxClosed
        else if !tx.writable then 
            throw DB.exNotAllowOp
        if key == null || key.length == 0 then
            return None 
        
        val c = new BTreeRawBucketIter(this)
        c.search(key) match 
            case (None,_) => None 
            case (Some(k,v),f) =>
                if key.sameElements(k) && f == Node.flagBucket then
                    throw new Exception("not allow delete subbucket value by delete method")
        c.node() match 
            case None => None 
            case Some(node) =>
                if node.del(key) then
                    bkv.count -= 1
                None
    //
    def delete(keys:Seq[Array[Byte]]):Unit = 
        if tx.closed then
            throw DB.exTxClosed
        else if !tx.writable then 
            throw DB.exNotAllowOp
        if keys == null || keys.length == 0 then
            return None
        
        val c = new BTreeRawBucketIter(this)
        for key <- keys if (key != null && key.length > 0) do
            c.search(key) match 
                case (None,_) => None 
                case (Some(k,v),f) =>
                    if key.sameElements(k) && f == Node.flagBucket then
                        throw new Exception("not allow delete subbucket value by delete method")
            c.node() match 
                case None => None 
                case Some(node) =>
                    if node.del(key) then
                        bkv.count -= 1
                    None
    /**
      * getBucket method retrieve a sub bucket in current bucket.
      * The returned bucket instance is only valid during transaction current lifecycle.
      * @param name: subbucket name
      * @return subbucket
      */
    def getBucket(name:Array[Byte]):Option[RawBucket] = getBucket(name,Collection.typeRawBucket)
    /**
      * createBucket try to create a new bucket and return it.
      * The create operation will failed if the key is already exists,or the name parameter is null or too large
      * The returned bucket instance is only valid during current transaction lifecycle.
      * @param name: bucket name
      * @return subbucket
      */
    def createBucket(name:Array[Byte]):Option[RawBucket] = createBucket(name,Collection.typeRawBucket)
    /**
      * create a new bucket if it doesn't exist,if already exists or create success then return it. 
      * create operation will failed if name is null or too large.
      * The returned bucket instance is only valid during current transaction lifecycle.
      * @param name: bucket name
      * @return subbucket
      */
    def createBucketIfNotExists(name:Array[Byte]):Option[RawBucket] = createBucketIfNotExists(name,Collection.typeRawBucket)
    /**
      * delete a subbucket.
      * delete opreation will failed if the bucket doesn't exist.
      * @param name: subbucket name
      * @return success flag
      */
    def deleteBucket(name:Array[Byte]):Unit = deleteBucket(name,Collection.typeRawBucket)
    /** 
     * try to get node or block by block id.
     * 
     */
    def nodeOrBlock(id:Long):(Option[RawNode],Option[Block]) = 
        if nodes.contains(id) then 
            (nodes.get(id),None)
        else 
            if id < 0 then
                // if id <0 means the bucket is a new created in memory, not loaded from disk.
                return (root,None)
            tx.block(id) match
                case Success(bk) => (None,Some(bk))
                case Failure(e) => (None,None)
    /**
      * parse node elements info from block.
      *
      * @param bk
      * @return
      */
    def nodeElements(bk:Option[Block]):Option[ArrayBuffer[RawNodeElement]] = 
        bk match
            case None => None 
            case Some(block) => 
                if nodes.contains(block.id) then
                    return Some(nodes(block.id).elements)
                else
                    Node.rawElements(block)
    /**
      * try to get the idx node element from the block. 
      *
      * @param bk
      * @param idx
      * @return
      */
    def getNodeElement(bk:Option[Block],idx:Int):Option[RawNodeElement] = 
        nodeElements(bk) match
            case None => None
            case Some(elems) =>
                if idx >= 0 && elems.length > idx then 
                    return Some(elems(idx))
                None
    /**
      * convert block to node. 
      *
      * @param bk
      * @return
      */
    def getNodeByBlock(bk:Try[Block]):Option[RawNode] = 
        bk match
            case Failure(_) => None 
            case Success(block) => 
                if nodes.contains(block.id) then 
                    nodes.get(block.id)
                Node.rawNode(block) match
                    case None => None 
                    case Some(node) => 
                        nodes(node.id) = node
                        Some(node)
    /**
      *  get node by id, search from cache -> disk.
      *
      * @param id
      * @return
      */
    def getNode(id:Long):Option[RawNode] = 
        if nodes.contains(id) then 
            return nodes.get(id)
        getNodeByBlock(tx.block(id))
    
    /**
      * try to get a child node by index. 
      *
      * @param n
      * @param idx
      * @return
      */
    def getNodeChild(n:Option[RawNode],idx:Int):Option[RawNode] = 
        n match
            case None => None
            case Some(node) =>
                if node.isLeaf || idx < 0 || idx >= node.length then
                    return None
                getNode(node.elements(idx).child) match
                    case None => None
                    case Some(child) =>
                        child.parent = Some(node)
                        node.children += child
                        Some(child)
    /**
      * try to get right brother node. 
      *
      * @param node
      * @return
      */
    private def getNodeRightSibling(node:RawNode):Option[RawNode] = 
        node.parent match
            case None => None 
            case Some(p) =>
                val idx = p.childIndex(node)
                if idx >= 0 && idx < p.length-1 then 
                    getNodeChild(Some(p),idx+1)
                else
                    None

    /**
      *  try to get node left brother node. 
      *
      * @param node
      * @return
      */
    private def getNodeLeftSibling(node:RawNode):Option[RawNode] =
        node.parent match
            case None => None 
            case Some(p) =>
                val idx = p.childIndex(node)
                if idx >= 1 then 
                    getNodeChild(Some(p),idx-1)
                else
                    None
    /**
     * rebalance the bucket,merge some small nodes.
     * 
     */
    def merge():Unit = 
        for (_,node) <- nodes do mergeOnNode(node)
        for (_,bk) <- buckets do bk.merge()
    /**
      * try to merge the node.
      *
      * @param node
      */  
    private def mergeOnNode(node:RawNode):Unit =
        if !node.unbalanced then return None
        node.unbalanced = false
        // check whether the node meets the threshold.
        val threshold:Int = DB.pageSize / 4
        if node.size() > threshold && node.length > Node.lowerBound(node.ntype) then
            return None
        // whether the current node is the root node.
        node.parent match
            case None =>
                /**
                  * If the root node is a branch node and has only one child node, then directly promote the child node to the new root node
                  * (obviously, if the root node is a leaf node, even if it contains one element, it does not need to be processed)
                  */
                if !node.isLeaf && node.length == 1 then 
                    getNodeChild(Some(node),0) match
                        case None => throw new Exception("merge root node failed: query child error")
                        case Some(child) => 
                            child.parent = None // set the child as new root and release old root
                            bkv.root = child.id
                            root = Some(child)
                            node.removeChild(child)
                            nodes.remove(node.id)
                            freeNode(node)
                    return None
            case Some(p) => 
                if node.length == 0 then // If node has no keys then just remove it.
                    p.del(node.minKey)
                    p.removeChild(node)
                    nodes.remove(node.id)
                    freeNode(node)
                    mergeOnNode(p)
                    return None
                // merge current node to right or left brother node.

                // The current node is the leftmost node of its parent node, 
                // so its right sibling node needs to be merged into the current node.
                if p.childIndex(node) == 0 then
                    getNodeRightSibling(node) match
                        case None => throw new Exception("merge node failed: get right brother node error")
                        case Some(mergeFrom) => 
                            for elem <- mergeFrom.elements do 
                                // If there are mergeFrom child nodes in the cache,then reset them parent as current node.
                                if nodes.contains(elem.child) then
                                    var child = nodes(elem.child)
                                    mergeFrom.removeChild(child)
                                    node.children += child
                                    child.parent = Some(node)
                            // remove all elements from mergeFrom to current node.
                            node.elements ++= mergeFrom.elements
                            p.del(mergeFrom.minKey)
                            p.removeChild(mergeFrom)
                            nodes.remove(mergeFrom.id)
                            freeNode(mergeFrom)
                else
                    getNodeLeftSibling(node) match // Merges the current node into its left sibling.
                        case None => throw new Exception("merge node failed: get left brother node error")
                        case Some(mergeTo) => 
                            for elem <- node.elements do 
                                if nodes.contains(elem.child) then 
                                    var child = nodes(elem.child)
                                    node.removeChild(child)
                                    mergeTo.children += child
                                    child.parent = Some(mergeTo)
                            // remove all elements from current node to mergeTo.
                            mergeTo.elements ++= node.elements
                            p.del(node.minKey)
                            p.removeChild(node)
                            nodes.remove(node.id)
                            freeNode(node)
                // Recursively processes the parent node of the current node.
                mergeOnNode(p)
    /**
      * rebalance bucket,split too large nodes.
      */
    def split():Unit =
        // split all cache subbuckets.
        val c = new BTreeRawBucketIter(this)
        for (name,bucket) <- buckets do 
            bucket.split()
            // split operation maybe change the sub-bucket's root,so need update the newest root info in current bucket.
            val nv = bucket.value
            bucket.root match 
                case None => None // Skip writing the bucket if there are no materialized nodes.
                case Some(n) =>
                    val key = Base64.getDecoder().decode(name)
                    c.search(key) match 
                        case (None,_) => throw new Exception(s"misplaced bucket header:$name")
                        case (Some(k,_),flag) =>
                            if !key.sameElements(k) then 
                                throw new Exception(s"misplaced bucket header:$name")
                            if flag != Node.flagBucket then 
                                throw new Exception(s"unexpected bucket header: $name flag:$flag")
                            c.node() match 
                                case Some(node) => node.put(k,key,nv.getBytes,Node.flagBucket,0)
                                case None => throw new Exception(s"not found leaf node for bucket element:$name")
        // split current bucket
        root match
            case None => return None // Ignore if there's not a materialized root node.
            case Some(node) => splitOnNode(node)
        
        // update root info.
        root match
            case None => None
            case Some(node) =>
                var r = node.root
                if r.id >= tx.maxPageId then
                    throw new Exception(s"pgid ${r.id} above high water mark ${tx.maxPageId}")
                bkv.root = r.id 
                root = Some(r)
    /**
      * split node recursively.
      *
      * @param node
      */
    private def splitOnNode(node:RawNode):Unit =
        if node.spilled then return None
        // Recursively slice the children of the current node. 
        // Note that child node splitting may add more elements to the current node's children array, 
        // and these new elements do not need to be repartitioned, so subscripts are recycled here.
        val n = node.children.length
        for i <- 0 until n do 
            splitOnNode(node.children(i))
        
        // We no longer need the child list because it's only used for spill tracking.
        if node.children.length != 0 then
            node.children = new ArrayBuffer[RawNode]()
        // split current node.
        for n <- splitNode(node,DB.pageSize) do 
            if n.id > 0 then 
                tx.free(n.id)
                n.header.pgid = 0
            
            // allocate a new block and write node content to it.
            val nid = tx.allocate(n.size()) 
            if nid >= tx.maxPageId then 
                throw new Exception(s"pgid $nid above high water mark ${tx.maxPageId}")
            var bk = tx.makeBlock(nid,n.size())
            n.header.pgid = bk.id
            n.writeTo(bk)
            n.spilled = true
            // insert the new node info to its parent.
            n.parent match
                case None => None
                case Some(p) => 
                    var k = n.minKey
                    if k == null || k.length == 0 then k = n.elements(0).key 
                    p.put(k,n.elements(0).key,null,0,n.id)
                    n.minKey = n.elements(0).key
        
        // if the old root was splitd and created a new one, we need split it as well.
        node.parent match
            case None => None
            case Some(p) => 
                if p.id <= 0 then
                    node.children = new ArrayBuffer[RawNode]()
                    splitOnNode(p)
    /**
      * Divide the node into several nodes according to the size.
      */
    private def splitNode(node:RawNode,sz:Int):List[RawNode] = 
        // split current node to two nodes, then split the second recursively.
        cutNode(node,sz) match
            case (head,None) => List(head)
            case (head,Some(tail)) => List(head):::splitNode(tail,sz)
    /**
      * split node to two nodes.
      *
      * @param node
      * @param sz
      * @return
      */
    private def cutNode(node:RawNode,sz:Int):(RawNode,Option[RawNode]) =
        if node.size() <= sz || node.length <= Node.minKeysPerBlock*2 then 
            return (node,None)
        
        val threshold = (sz*DB.fillPercent).toInt
        var n = BlockHeader.size
        var idx = -1
        breakable(
            for i <- 0 until node.length do
                n += Node.indexSize + node.elements(i).keySize + node.elements(i).valueSize
                if n >= threshold then 
                    idx = i 
                    break()
        )
        if idx < 0 then
            return (node,None)
         
        var nodeB = new RawNode(new BlockHeader(-1L,node.header.flag,0,0,0))
        nodeB.elements = node.elements.slice(idx,node.elements.length)

        node.elements = node.elements.slice(0,idx)
        // if current node's parent is null, then create a new one.
        node.parent match
            case Some(p) => 
                nodeB.parent = Some(p)
                p.children += nodeB
            case None =>
                var parent = new RawNode(new BlockHeader(-1L,Block.typeBranch,0,0,0))
                parent.children += node
                parent.children += nodeB
                node.parent = Some(parent)
                nodeB.parent = Some(parent)
        (node,Some(nodeB))

    /** 
     * release pages of node. 
     * 
     */
    private def freeNode(node:RawNode):Unit = 
        if node.id > DB.meta1Page then
            tx.free(node.id)
            node.header.pgid = 0
            nodes.remove(node.id)

    /** 
     * release all pages of  node and its child nodes. 
     * 
     */
    private def freeFrom(id:Long):Unit = 
        if id <= 0 then return None 
        nodeOrBlock(id) match
            case (None,None) => None 
            case (Some(node),_) =>
                freeNode(node)
                if !node.isLeaf then 
                    for elem <- node.elements do freeFrom(elem.child)
            case (None,Some(bk)) =>
                tx.free(bk.id)
                if bk.header.flag != Block.typeLeaf then 
                    nodeElements(Some(bk)) match 
                        case None => None
                        case Some(elems) =>
                            for elem <- elems do freeFrom(elem.child)
        None 

    /** 
     * release all pages about current bucket. 
     *
     */
    def freeAll():Unit =
        freeFrom(bkv.root)
        bkv.root = 0

    // try to get a sub-bucket.
    private def searchBucket(c:BTreeRawBucketIter,name:Array[Byte],dataType:Byte):Option[BTreeRawBucket] = 
        c.search(name) match
            case (None,_) => None
            case (Some(k,v),f) => 
                if !name.sameElements(k) || f != Node.flagBucket then 
                    throw new Exception("not found bucket")
                BTreeBucket.getValue(v) match
                    case None => 
                        throw new Exception(s"parse value failed,expect data length is ${BTreeBucket.valueSize} but actual get ${v}") 
                    case Some(value) =>
                        if value.dataType != dataType then
                            throw new Exception(s"already exists collection data type is ${Collection.typeName(value.dataType)}")
                        var bk = new BTreeRawBucket(name,tx)
                        bk.bkv = value 
                        buckets(Base64.getEncoder().encodeToString(name)) = bk
                        Some(bk)
    /**
      * 
      *
      * @param name
      * @param dataType
      * @return
      */
    private def getBucket(name:Array[Byte],dataType:Byte):Option[BTreeRawBucket] = 
        if tx.closed then
            throw DB.exTxClosed
        if name == null || name.length == 0 then 
            return None 

        val sn = Base64.getEncoder().encodeToString(name)
        if buckets.contains(sn) then 
            return buckets.get(sn)
        
        val c = new BTreeRawBucketIter(this)
        c.search(name) match
            case (None,_) => None
            case (Some(k,v),f) => 
                if !name.sameElements(k) || f != Node.flagBucket then 
                    return None
                BTreeBucket.getValue(v) match
                    case None => throw new Exception(s"parse value failed,expect data length is ${BTreeBucket.valueSize} but actual get ${v}") 
                    case Some(value) =>
                        if value.dataType != dataType then
                            throw new Exception(s"already exists collection data type is ${Collection.typeName(value.dataType)}")
                        var bk = new BTreeRawBucket(name,tx)
                        bk.bkv = value 
                        buckets(sn) = bk
                        Some(bk)
    /**
      * 
      *
      * @param name
      * @param dataType
      * @return
      */
    private def createBucket(name:Array[Byte],dataType:Byte):Option[BTreeRawBucket] = 
        if tx.closed then
            throw DB.exTxClosed
        else if !tx.writable then 
            throw DB.exNotAllowOp
        else if name == null || name.length == 0 then 
            throw DB.exKeyIsNull
        else if name.length >= DB.maxKeySize then
            throw DB.exKeyTooLarge
        
        val sn = Base64.getEncoder().encodeToString(name)
        if buckets.contains(sn) then 
            throw new Exception("already exists bucket or region name")
        
        val c = new BTreeRawBucketIter(this)
        c.search(name) match
            case (None,_) => None
            case (Some(k,v),f) => 
                if name.sameElements(k) && f != Node.flagBucket then
                    throw new Exception("already exists key name")
                if name.sameElements(k) && f == Node.flagBucket then
                    BTreeBucket.getValue(v) match
                        case None => throw new Exception("parse collection value failed")
                        case Some(value) => throw new Exception(s"already exists ${Collection.typeName(value.dataType)} collection name")
                    throw new Exception("already exists collection name")
        // create a new bucket
        var bk = new BTreeRawBucket(name,tx)
        bk.bkv = new BucketValue(-1,0,0,dataType) // empty bkv
        bk.root = Some(new RawNode(new BlockHeader(-1L,Block.typeLeaf,0,0,0))) // empty root node
        buckets(sn) = bk
        c.node() match 
            case None => throw new Exception("create failed: not found create node")
            case Some(n) =>
                if !n.put(name,name,bk.value.getBytes,Node.flagBucket,0) then
                    bkv.count += 1
                Some(bk)
    /**
      * 
      *
      * @param name
      * @param dataType
      * @return
      */
    private def createBucketIfNotExists(name:Array[Byte],dataType:Byte):Option[BTreeRawBucket] =
        if tx.closed then
            throw DB.exTxClosed
        else if !tx.writable then 
            throw DB.exNotAllowOp
        else if name == null || name.length == 0 then 
            throw DB.exKeyIsNull
        else if name.length >= DB.maxKeySize then
            throw DB.exKeyTooLarge

        getBucket(name,dataType) match
            case Some(bk) => Some(bk)
            case None => createBucket(name,dataType)
    /**
      * 
      *
      * @param name
      * @param dataType
      * @return
      */
    private def deleteBucket(name:Array[Byte],dataType:Byte):Unit =
        if tx.closed then
            throw DB.exTxClosed
        else if !tx.writable then 
            throw DB.exNotAllowOp
        else if name == null || name.length == 0 then 
            return None 
        else if name.length >= DB.maxKeySize then
            throw DB.exKeyTooLarge

        var obk:Option[BTreeRawBucket] = None
        val c = new BTreeRawBucketIter(this)
        val sn = Base64.getEncoder().encodeToString(name)
        if buckets.contains(sn) then
            obk = buckets.get(sn) 
        else
            c.search(name) match 
                case (None,_) => None 
                case (Some(k,v),f) => 
                    // key not exists or exists but not a bucket
                    if !name.sameElements(k) then 
                        return None
                    if f != Node.flagBucket then
                        throw new Exception(s"$sn is not a collection")
                    obk = searchBucket(c,name,dataType)
        obk match
            case Some(bk) => 
                // delete subbuckets recursively.
                if dataType == Collection.typeRawBucket then
                    for kv <- bk.iterator do kv match
                        case Some((k,v)) => 
                            if (v != null && v.sameElements(DB.magicBytes)) then 
                                bk.deleteBucket(k,Collection.typeRawBucket)
                        case None => None
                // delete current bucket
                buckets.remove(sn) // clean cache
                bk.nodes.clear()  // clean cache nodes
                bk.root = None 
                bk.freeAll() // release all pages about the bucket
                // delete bucket record from the parent node.
                c.find(name)
                c.node() match 
                    case None => 
                        throw new Exception(s"not found ${Collection.typeName(dataType)} $sn node")
                    case Some(node) => 
                        if node.del(name) then
                            bkv.count -= 1
                        None
            case None => None
    //
    def clean():Unit = 
        if tx.closed then
            throw DB.exTxClosed
        else if !tx.writable then 
            throw DB.exNotAllowOp
        
        val c = new BTreeRawBucketIter(this)
        for kv <- c do kv match
            case Some(k,v) => 
                if (v != null && v.sameElements(DB.magicBytes)) then 
                    deleteBucket(k,Collection.typeRawBucket)
            case None => None 
        nodes.clear()
        freeAll() // release all pages about the bucket
        bkv = new BucketValue(-1,0,0,Collection.typeRawBucket) // empty bkv
        root = Some(new RawNode(new BlockHeader(-1L,Block.typeLeaf,0,0,0))) // empty root node
        None
        // TODO:update the buckets record

trait RawIterator extends Iterator[Option[(Array[Byte],Array[Byte])]]:
    def find(key:Array[Byte]):Option[(Array[Byte],Array[Byte])]
    def first():Option[(Array[Byte],Array[Byte])]
    def last():Option[(Array[Byte],Array[Byte])]
    def hasNext():Boolean
    def next():Option[(Array[Byte],Array[Byte])]
    def hasPrev():Boolean 
    def prev():Option[(Array[Byte],Array[Byte])]

private class RawRecord(var node:Option[RawNode],var block:Option[Block],var index:Int):
    def isLeaf:Boolean =(node,block) match
        case (None,None) => false
        case (Some(n),_) => n.isLeaf
        case (_,Some(b)) => b.header.flag == Block.typeLeaf
    def isBranch:Boolean = (node,block) match
        case (None,None) => false
        case (Some(n),_) => n.isBranch
        case (_,Some(b)) => b.header.flag == Block.typeBranch
    def count:Int = (node,block) match
        case (None,None) => 0
        case (Some(n),_) => n.length
        case (_,Some(b)) => b.header.count

private class BTreeRawBucketIter(private var bucket:BTreeRawBucket) extends RawIterator:
    // use a stack to record serach path.
    private var stack:ArrayBuffer[RawRecord] = new ArrayBuffer[RawRecord]()
    private var idx:Int = 0
    private def top:Option[RawRecord] = if idx > 0 then Some(stack(idx-1)) else None
    private def empty:Boolean = idx == 0
    private def pop:RawRecord = 
        if idx > 0 then 
            val r = stack(idx-1)
            idx -= 1
            r 
        else 
            null 
    private def push(r:RawRecord):Unit = 
        if idx == stack.length then 
            stack.append(r)
        else 
            stack(idx) = r 
        idx += 1
    private def clear():Unit = idx = 0
    override def size: Int = bucket.length.toInt
    override def knownSize: Int = bucket.length.toInt
    //
    def find(key:Array[Byte]):Option[(Array[Byte],Array[Byte])] = 
        if bucket.closed then 
            None 
        else
            clear()
            seek(key,bucket.bkv.root)
            current() match 
                case Some(e) =>
                    if e.flag != Node.flagBucket then 
                        Some(e.key,e.value)
                    else
                        None    
                case None => None
    //
    def first():Option[(Array[Byte],Array[Byte])] = 
        if bucket.closed || bucket.length == 0 then 
            return None
        clear()
        val (n,b) = bucket.nodeOrBlock(bucket.bkv.root)
        push(new RawRecord(n,b,0))
        moveToFirst()
        top match
            case Some(r) => 
                if r.count == 0 then 
                    moveToNext()
                current() match
                    case Some(e) => 
                        if e.flag == Node.flagBucket then 
                            Some(e.key,DB.magicBytes)
                        else
                            Some(e.key,e.value)
                    case None => None
            case None => None
    //
    def next():Option[(Array[Byte],Array[Byte])] = 
        if bucket.closed || bucket.length == 0 then 
            None
        else
            moveToNext()
            current() match
                case None => None
                case Some(e) =>
                    if e.flag == Node.flagBucket then 
                        Some(e.key,DB.magicBytes)
                    else
                        Some(e.key,e.value)
    //
    def hasNext(): Boolean =
        if bucket.closed || bucket.length == 0 then
            false 
        else
            if empty then 
                val (n,b) = bucket.nodeOrBlock(bucket.bkv.root)
                push(new RawRecord(n,b,-1))
            var i = idx-1
            while i >= 0 do 
                val r = stack(i)
                // whenever leaf node and branch node, 
                // as long as the index not traverse all element,then there must be successor elements.
                if r.index < r.count-1 then 
                    return true 
                i-=1
            false 
    //
    def last():Option[(Array[Byte],Array[Byte])] = 
        if bucket.closed || bucket.length == 0 then 
            return None
        clear()
        val (n,b) = bucket.nodeOrBlock(bucket.bkv.root)
        var r = new RawRecord(n,b,0)
        r.index = r.count-1
        push(r)
        moveToLast()

        top match
            case Some(r) => 
                if r.count == 0 then 
                    moveToPrev()
                current() match
                    case None => None
                    case Some(e) =>
                        if e.flag == Node.flagBucket then 
                            Some(e.key,DB.magicBytes)
                        else
                            Some(e.key,e.value)
            case None => None
    //
    def prev():Option[(Array[Byte],Array[Byte])] = 
        if bucket.closed || bucket.length == 0 then 
            return None
        moveToPrev()
        current() match
            case None => None
            case Some(e) =>
                if e.flag == Node.flagBucket then 
                    Some(e.key,DB.magicBytes)
                else
                    Some(e.key,e.value)
    //
    def hasPrev():Boolean = 
        if bucket.closed || bucket.length == 0 then 
            false 
        else
            if empty then
                val (n,b) = bucket.nodeOrBlock(bucket.bkv.root)
                var r = new RawRecord(n,b,0)
                r.index = r.count
                push(r)
            var i = idx-1
            while i >= 0 do 
                val r = stack(i)
                if r.index > 0 then
                    return true 
                i -= 1 
            false
    //
    private[platdb] def search(key:Array[Byte]):(Option[(Array[Byte],Array[Byte])],Byte) =
        if bucket.closed then
            (None,0)
        else
            clear()
            seek(key,bucket.bkv.root)
            current() match
                case None => (None,0)
                case Some(e) => (Some(e.key,e.value),e.flag)
    //
    private[platdb] def node():Option[RawNode] = 
        if empty then
            None 
        else
            top match
                case None => None 
                case Some(r) => r.node match
                    case None => None 
                    case Some(node) => if r.isLeaf then return r.node
            val r = stack(0)
            var n:Option[RawNode] = (r.node,r.block) match
                case (None,None) => None
                case (Some(nd),_) => Some(nd)
                case (_,Some(bk)) =>
                    r.node = bucket.getNodeByBlock(Try(bk))
                    bucket.root match
                        case None => bucket.root = r.node
                        case Some(_) => None
                    r.node
            // top-down: convert blocks on search path to nodes.
            breakable(
                for i <- 0 until idx-1 do
                    val r = stack(i) 
                    bucket.getNodeChild(n,r.index) match
                        case Some(nd) => n = Some(nd) 
                        case None => break()
            )
            n match
                case None => None
                case Some(node) => if node.isLeaf then Some(node) else None
    //
    private def moveToFirst():Unit = 
        if !empty then
            var r = stack(idx-1)
            while r.isBranch do
                var child:Long = 0 
                r.node match
                    case Some(node) => 
                        if node.elements.length == 0 then 
                            return
                        child = node.elements(r.index).child
                    case None =>
                        bucket.getNodeElement(r.block,r.index) match
                            case Some(e) => child = e.child 
                            case None => return 
                if child > DB.meta1Page then 
                    val (n,b) = bucket.nodeOrBlock(child) 
                    push(new RawRecord(n,b,0))
                    r = stack(idx-1)
                else
                    throw new Exception(s"moveToFirst visit reversed page $child")
    //
    private def moveToLast():Unit =
        if !empty then
            var r = stack(idx-1)
            while r.isBranch do
                var child:Long = 0  
                r.node match
                    case Some(node) => 
                        if node.elements.length == 0 then 
                            return
                        child = node.elements(r.index).child
                    case None =>
                        bucket.getNodeElement(r.block,r.index) match
                            case Some(e) => child = e.child 
                            case None => return 
                if child > DB.meta1Page then 
                    val (n,b) = bucket.nodeOrBlock(child)  // page id 0 or 1 reserved for meta.
                    val p = new RawRecord(n,b,0)
                    p.index = p.count-1 
                    push(p)
                    r = stack(idx-1)
                else
                    throw new Exception(s"moveToLast visit reversed page $child")
    //
    private def moveToNext():Unit = 
        if !empty then 
            var r = stack(idx-1)
            while r.count == 0 || r.index >= r.count-1 do  // back to upper level.
                pop
                if empty then
                    return
                else
                    r = stack(idx-1)
            r.index += 1 // index move to next location,point to next subtree.
            stack(idx-1) = r 
            moveToFirst()
    //      
    private def moveToPrev():Unit =
        if !empty then
            var r = stack(idx-1)
            while r.count == 0 || r.index <= 0 do  // back to upper level.
                pop
                if empty then
                    return
                else
                    r = stack(idx-1)
            r.index -= 1 // index move to next location,point to next subtree.
            stack(idx-1) = r 
            moveToLast()
    //
    private def current():Option[RawNodeElement] = 
        if !empty then
            val r = stack(idx-1)
            if r.count == 0 || r.index >= r.count || r.index < 0 then 
                None
            else
                r.node match 
                    case Some(node) => Some(node.elements(r.index))
                    case None => bucket.getNodeElement(r.block,r.index) 
        else
            None
    //
    private def seek(key:Array[Byte],id:Long):Unit =
        val r = bucket.nodeOrBlock(id) match
            case (None,None) => throw new Exception(s"not found node or block for id:$id")
            case (Some(n),_) => new RawRecord(Some(n),None,0)
            case (_,Some(b)) =>
                if b.btype != Block.typeBranch && b.btype != Block.typeLeaf then 
                    throw new Exception(s"page ${id} invalid page type:${b.btype}")
                new RawRecord(None,Some(b),0)
        push(r)
        if r.isLeaf then 
            seekOnLeaf(key)
        else
            r.node match
                case Some(node) => seekOnNode(key,node)
                case None => seekOnBlock(key,r.block)
    //
    private def seekOnLeaf(key:Array[Byte]):Unit =
        var r = stack(idx-1)
        r.node match 
            case Some(node) =>
                val i = node.elements.findIndex(e => Util.compare(e.key, key) >= 0 ) 
                r.index = if i < 0 then node.length-1 else i
                stack(idx-1) = r
            case None =>
                bucket.nodeElements(r.block) match
                    case Some(elems) => 
                        val i = elems.findIndex(e => Util.compare(e.key, key) >= 0 )
                        r.index = if i < 0 then elems.length-1 else i
                        stack(idx-1) = r 
                    case None => None
    // search in branch node.
    private def seekOnNode(key:Array[Byte],node:RawNode):Unit =
        var i = node.elements.findIndex(e => Util.compare(e.key, key) >= 0)
        if i < 0 then
            i = node.elements.length - 1
        else
            if i > 0 && node.elements(i).key != key then i -= 1
        stack(idx-1).index = i
        seek(key,node.elements(i).child)
    
    // search in branch block.
    private def seekOnBlock(key:Array[Byte],block:Option[Block]):Unit =
        bucket.nodeElements(block) match
            case None => None
            case Some(elems) =>
                var i:Int = elems.findIndex(e => Util.compare(e.key, key) >= 0)
                if i < 0 then
                    i = elems.length - 1
                else
                    if i > 0 && elems(i).key != key then i -= 1
                stack(idx-1).index = i
                seek(key,elems(i).child)
