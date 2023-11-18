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

import scala.util.Try
import scala.util.control.Breaks._

/**
  * CollectionIterator is used to traverse platdb data structs.
  */
trait CollectionIterator extends Iterator[(Option[String],Option[String])]:
    /**
      * Retrieves the specified element.
      *
      * @param key
      * @return
      */
    def find(key:String):(Option[String],Option[String])
    /**
      * moves the iterator to the first item in the bucket and returns its key and value.
      *
      * @return
      */
    def first():(Option[String],Option[String]) 
    /**
      * moves the iterator to the last item in the bucket and returns its key and value.
      *
      * @return
      */
    def last():(Option[String],Option[String]) 
    /**
      * Determine if there is the next element of the current iterator.
      *
      * @return
      */
    def hasNext():Boolean
    /**
      * moves the iterator to the next item in the bucket and returns its key and value.
      *
      * @return
      */
    def next():(Option[String],Option[String])
    /**
      * Determine whether the current iterator has the previous element.
      *
      * @return
      */
    def hasPrev():Boolean 
    /**
      * moves the iterator to the previous item in the bucket and returns its key and value.
      *
      * @return
      */
    def prev():(Option[String],Option[String]) 

/**
  * Platdb iterable object.
  */
trait Iterable:
    /**
      * the elements number of current collection.
      *
      * @return
      */
    def length:Long
    /**
      * return a collection iterator object.
      *
      * @return
      */
    def iterator:CollectionIterator


private[platdb] class Record(var node:Option[Node],var block:Option[Block],var index:Int):
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
    def count:Int =
        (node,block) match
            case (None,None) => 0
            case (Some(n),_) => n.length
            case (_,Some(b)) => b.header.count
/**
  * 
  *
  * @param bucket
  */
private[platdb] class BTreeBucketIter(private var bucket:BTreeBucket) extends CollectionIterator:
    // use a stack to record serach path.
    private var stack:List[Record] = List[Record]()
    /**
      * moves the iterator to a given key and returns it.
      * If the key does not exist then the next key is used. If no keys follow, a None key is returned.
      * The returned key and value are only valid for the life of the transaction.
      *
      * @param key
      * @return
      */
    def find(key:String):(Option[String],Option[String]) = 
        if bucket.closed then return (None,None)
        stack = List[Record]()
        seek(key,bucket.bkv.root)
        current() match 
            case None => (None,None)
            case Some(e) =>
                if e.flag == bucketType then 
                    (Some(e.key),None)
                else
                    (Some(e.key),Some(e.value))
    /**
      * moves the iterator to the first item in the bucket and returns its key and value.
      * If the bucket is empty then a None key and None value are returned.
      * The returned key and value are only valid for the life of the transaction.
      *
      * @return
      */
    def first():(Option[String],Option[String]) = 
        if bucket.closed then return (None,None)
        stack = List[Record]()
        
        val (n,b) = bucket.nodeOrBlock(bucket.bkv.root)
        stack:+=new Record(n,b,0)
        moveToFirst()
        
        val r = stack.last
        if r.count == 0 then // if located to a null node, need move to next item.
            moveToNext()
        
        current() match
            case None => (None,None)
            case Some(e) =>
                if e.flag == bucketType then 
                    (Some(e.key),None)
                else
                    (Some(e.key),Some(e.value))
    /**
      * iterator move to next leaf node element, return the key and value.
      * if the element is a subbucket,then the value is None.
      *
      * @return
      */
    def next():(Option[String],Option[String]) = 
        if bucket.closed then return (None,None)
        moveToNext()
        current() match
            case None => (None,None)
            case Some(e) =>
                if e.flag == bucketType then 
                    (Some(e.key),None)
                else
                    (Some(e.key),Some(e.value))
    /**
      * check if has successor elements in current buckets.
      *
      * @return
      */
    def hasNext(): Boolean =
        if bucket.closed then 
            return false
        if stack.length == 0 then
            val (n,b) = bucket.nodeOrBlock(bucket.bkv.root)
            stack:+=new Record(n,b,-1)

        var tmpStack = List[Record]()
        tmpStack ++= stack
        while tmpStack.length > 0 do 
            val r = tmpStack.last
            // whenever leaf node and branch node, 
            // as long as the index not traverse all element,then there must be successor elements.
            if r.index < r.count-1 then 
                return true 
            tmpStack = tmpStack.init
        false 
    /**
      * moves the iterator to the latest item in the bucket and returns its key and value.
      * If the bucket is empty then a None key and None value are returned.
      * The returned key and value are only valid for the life of the transaction.
      *
      * @return
      */
    def last():(Option[String],Option[String])  = 
        if bucket.closed then return (None,None)
        stack = List[Record]()
        val (n,b) = bucket.nodeOrBlock(bucket.bkv.root)
        var r = new Record(n,b,0)
        r.index = r.count-1
        moveToLast()

        r = stack.last
        if r.count == 0 then 
            moveToPrev()
        
        current() match
            case None => (None,None)
            case Some(e) =>
                if e.flag == bucketType then 
                    (Some(e.key),None)
                else
                    (Some(e.key),Some(e.value))
    /**
      * iterator move to prev element, return the key and value.
      * if the element is a subbucket,then the value is None.
      *
      * @return
      */
    def prev():(Option[String],Option[String]) = 
        if bucket.closed then return (None,None) 
        moveToPrev()
        current() match
            case None => (None,None)
            case Some(e) =>
                if e.flag == bucketType then 
                    (Some(e.key),None)
                else
                    (Some(e.key),Some(e.value))
    /**
      * 
      *
      * @return
      */
    def hasPrev():Boolean = 
        if bucket.closed then 
            return false 
        if stack.length == 0 then
            val (n,b) = bucket.nodeOrBlock(bucket.bkv.root)
            var r = new Record(n,b,0)
            r.index = r.count
            stack:+=r
        var tmpStack = List[Record]()
        tmpStack++=stack
        while tmpStack.length > 0 do 
            val r = tmpStack.last
            // whenever leaf node and branch node, 
            // as long as the index not reverse traverse all element,then there must be precursor elements.
            if r.index > 0 then 
                return true
            tmpStack = tmpStack.init
        false 
    /**
      * search elements by key in current bucekt, return key, value and value type info.
      *
      * @param key
      * @return
      */
    private[platdb] def search(key:String):(Option[String],Option[String],Byte) =
        if bucket.closed then return (None,None,0)
        stack = List[Record]()
        seek(key,bucket.bkv.root)
        current() match
            case None => (None,None,0)
            case Some(e) => (Some(e.key),Some(e.value),e.flag)
    /**
      * return current stack top node.
      *
      * @return
      */
    private[platdb] def node():Option[Node] = 
        if stack.length == 0 then return None 
        var r = stack.last
        r.node match 
            case Some(node) =>
                if r.isLeaf then
                    return r.node 
            case None => None
        
        var n:Option[Node] = None
        r = stack.head
        (r.node,r.block) match
            case (None,None) => return None
            case (Some(nd),_) => n = Some(nd)
            case (_,Some(bk)) =>
                r.node = bucket.getNodeByBlock(Try(bk))
                bucket.root match
                    case None => bucket.root = r.node
                    case Some(_) => None
                n = r.node

        // top-down: convert blocks on search path to nodes.
        breakable(
            for r <- stack.init do 
                bucket.getNodeChild(n,r.index) match
                    case Some(nd) => n = Some(nd) 
                    case None => break()
        )
        n match
            case None => None
            case Some(node) =>
                if !node.isLeaf then None else Some(node)
    /**
      * from stack top element, top-down move to the rightest leaf node of the subtree. 
      */
    private def moveToFirst():Unit = 
        if stack.length == 0 then return
        var r = stack.last
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
            if child>DB.meta1Page then 
                val (n,b) = bucket.nodeOrBlock(child) 
                stack :+= new Record(n,b,0)
                r = stack.last
            else
                throw new Exception(s"moveToFirst visit reversed page $child")
    /**
      * move to current subtree rightest element.
      */
    private def moveToLast():Unit =
        if stack.length == 0 then return
        var r = stack.last
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
            if child>DB.meta1Page then 
                val (n,b) = bucket.nodeOrBlock(child)  // page id 0 or 1 reserved for meta.
                r = new Record(n,b,0)
                r.index = r.count-1 
                stack :+= r
            else
                throw new Exception(s"moveToLast visit reversed page $child")
    /**
      * move iterator to next leaf node element location.
      */
    private def moveToNext():Unit = 
        if stack.length == 0 then return
        var r = stack.last 
        while r.count == 0 || r.index >= r.count-1 do // back to upper level.
            stack = stack.init
            if stack.length == 0 then 
                return 
            r = stack.last
        r.index = r.index+1 // index move to next location,point to next subtree.
        stack = stack.init
        stack:+=r
        moveToFirst()
    /**
      * move the iterator to prever leaf node element location.
      */       
    private def moveToPrev():Unit =
        if stack.length == 0 then return
        var r = stack.last 
        while r.count == 0 || r.index <= 0 do
            stack = stack.init
            if stack.length == 0 then 
                return 
            r = stack.last
        r.index = r.index-1 // index point to prev subtree. 
        stack = stack.init
        stack :+= r  
        moveToLast()
    /**
      * 
      *
      * @return
      */
    private def current():Option[NodeElement] = 
        val r = stack.last
        if r.count == 0 || r.index >= r.count || r.index<0 then 
            return None
        r.node match 
            case Some(node) => Some(node.elements(r.index))
            case None => bucket.getNodeElement(r.block,r.index) 
    /**
      * serach element from node id.
      *
      * @param key
      * @param id
      */
    private def seek(key:String,id:Long):Unit =
        var r:Record = null
        bucket.nodeOrBlock(id) match
            case (None,None) => throw new Exception(s"not found node or block for id:$id")
            case (Some(n),_) => r = new Record(Some(n),None,0)
            case (_,Some(b)) =>
                if b.btype!= branchType && b.btype!=leafType then 
                    throw new Exception(s"page ${id} invalid page type:${b.btype}")
                r = new Record(None,Some(b),0)
        stack:+=r
        if r.isLeaf then 
            seekOnLeaf(key)
        else
            r.node match
                case Some(node) => seekOnNode(key,node)
                case None => seekOnBlock(key,r.block)
    /**
      * search in leaf node.
      *
      * @param key
      */
    private def seekOnLeaf(key:String):Unit =
        var r = stack.last
        r.node match 
            case Some(node) =>
                val idx = node.elements.indexFunc((e:NodeElement) => e.key>=key) 
                if idx < 0 then
                    r.index = node.length-1
                else
                    r.index = idx
                stack = stack.init
                stack :+= r
            case None =>
                bucket.nodeElements(r.block) match
                    case Some(elems) => 
                        val idx = elems.indexFunc((e:NodeElement) => e.key>=key)
                        if idx < 0 then
                            r.index = elems.length-1
                        else
                            r.index = idx
                        stack = stack.init
                        stack :+= r
                    case None => None
    
    // search in branch node.
    private def seekOnNode(key:String,node:Node):Unit =
        var idx = node.elements.indexFunc((e:NodeElement)=> e.key>=key)
        if idx < 0 then
            idx = node.elements.length - 1
        else
            if idx > 0 && node.elements(idx).key != key then
                idx -= 1

        var r = stack.last
        r.index = idx 
        stack = stack.init
        stack :+= r
        seek(key,node.elements(idx).child)
    
    // search in branch block.
    private def seekOnBlock(key:String,block:Option[Block]):Unit =
        bucket.nodeElements(block) match
            case None => None
            case Some(elems) =>
                var idx:Int = elems.indexFunc((e:NodeElement) => e.key>=key)
                if idx < 0 then
                    idx = elems.length - 1
                else
                    if idx > 0 && elems(idx).key != key then
                        idx -= 1
                
                var r= stack.last
                r.index = idx 
                stack = stack.init
                stack :+= r
                seek(key,elems(idx).child)
