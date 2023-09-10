package platdb

import scala.util.Try
import scala.util.control.Breaks._

trait BucketIterator extends Iterator[(Option[String],Option[String])]:
    def first():(Option[String],Option[String]) 
    def last():(Option[String],Option[String]) 
    def prev():(Option[String],Option[String]) 
    def find(key:String):(Option[String],Option[String])
    def next():(Option[String],Option[String])
    def hasNext():Boolean
    def hasPrev():Boolean 

private[platdb] class Record(var node:Option[Node],var block:Option[Block],var index:Int):
    def isLeaf:Boolean =
        node match
            case None => None
            case Some(n) => return n.isLeaf
        block match
            case None => None
            case Some(bk) => return bk.header.flag == leafType
        false
    def isBranch:Boolean =
        node match
            case None => None
            case Some(n) => return n.isBranch
        block match
            case None => None
            case Some(bk) => return bk.header.flag == branchType
        false
    def count:Int = 
        node match
            case None => None
            case Some(n) => return n.length
        block match
            case None => None
            case Some(bk) => return bk.header.count
        0

private[platdb] class bucketIter(private var bucket:Bucket) extends BucketIterator:
    private var stack:List[Record] = _
    
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
        stack::=new Record(n,b,0)
        moveToFirst()
        
        val r = stack.last
        if r.count == 0 then // if located to a null node, need move to next item.
            moveToNext()
        
        current() match
            case (None,_,_) => return (None,None)
            case (k,v,f) =>
                if f == bucketType then 
                    return (k,None)
                return (k,v)
    
    /**
      * from stack top element, top-down move to the rightest leaf node of the subtree. 
      */
    private def moveToFirst():Unit = 
        var r = stack.last
        while r.isBranch do
            var child:Int = 0  
            r.node match
                case Some(node) => 
                    if node.elements.length == 0 then 
                        return
                    child = node.elements(r.index).child
                case None =>
                    bucket.getNodeElement(r.block,r.index) match
                        case Some(e) => child = e.child 
                        case None => return 
            if child>1 then 
                val (n,b) = bucket.nodeOrBlock(child) 
                stack ::= new Record(n,b,0)
                r = stack.last
    /**
      * move to next leaf node.
      */
    private def moveToNext():Unit = 
        if stack.length == 0 then return None
        var r = stack.last 
        while r.count == 0 || r.index == r.count do // back to upper level.
            stack = stack.init
            if stack.length == 0 then 
                return 
            r = stack.last
        r.index = r.index+1 // index move to next location,point to next subtree.
        stack = stack.init
        stack::=r

        moveToFirst()

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
            case (None,_,_) => return (None,None)
            case (k,v,f) =>
                if f == bucketType then 
                    return (k,None)
                return (k,v)

    def hasNext(): Boolean =
        if bucket.closed || stack.length == 0 then 
            return false 
        
        var tmpStack = List[Record]()
        tmpStack :::=stack
        while tmpStack.length > 0 do 
            val r = tmpStack.last
            // whenever leaf node and branch node, as long as the index not traverse all element,then there must be successor elements.
            if r.index < r.count then 
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
            case (None,_,_) => return (None,None)
            case (k,v,f) =>
                if f == bucketType then 
                    return (k,None)
                return (k,v)
    
    /**
      * iterator move to prev leaf node element, return the key and value.
      * if the element is a subbucket,then the value is None.
      *
      * @return
      */
    def prev():(Option[String],Option[String]) = 
        if bucket.closed then return (None,None) 
        moveToPrev()
        current() match
            case (None,_,_) => return (None,None)
            case (k,v,f) =>
                if f == bucketType then 
                    return (k,None)
                return (k,v)

    def hasPrev():Boolean = 
        if bucket.closed || stack.length == 0 then 
            return false 
        
        var tmpStack= List[Record]()
        tmpStack :::=stack
        while tmpStack.length > 0 do 
            val r = tmpStack.last
            // whenever leaf node and branch node, as long as the index not reverse traverse all element,then there must be precursor  elements.
            if r.index > 0 then 
                return true
            tmpStack = tmpStack.init
        false 

    private def moveToLast():Unit =
        if stack.length == 0 then return
        var r = stack.last
        while r.isBranch do
            var child:Int = 0  
            r.node match
                case Some(node) => 
                    if node.elements.length == 0 then 
                        return
                    child = node.elements(r.index).child
                case None =>
                    bucket.getNodeElement(r.block,r.index) match
                        case Some(e) => child = e.child 
                        case None => return 
            if child>1 then 
                val (n,b) = bucket.nodeOrBlock(child)  // page id 0 or 1 reserved for meta.
                var r = new Record(n,b,0)
                r.index = r.count -1 
                stack ::= r
            
    private def moveToPrev():Unit =
        if stack.length == 0 then return
        var r = stack.last 
        while r.count == 0 || r.index == 0 do
            stack = stack.init
            if stack.length == 0 then 
                return 
            r = stack.last
        r.index = r.index-1 // index point to prev subtree. 
        stack = stack.init
        stack ::= r  
        
        moveToLast() // move to current subtree rightest element.
    
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
        
        val r = stack.last
        if r.index >= r.count then
            moveToNext()
        
        current() match 
            case (None,_,_) => return (None,None)
            case (Some(k),v,f) =>
                if f == bucketType then 
                    return (Some(k),None)
                return (Some(k),v)

    private[platdb] def search(key:String):(Option[String],Option[String],Int) =
        if bucket.closed then return (None,None,0)
        stack = List[Record]()
        seek(key,bucket.bkv.root)
        val r =stack.last
        if r.index >= r.count then 
            moveToNext()
        current() 

    // current
    private def current():(Option[String],Option[String],Int) = 
        val r= stack.last
        var e:NodeElement = null
        if r.count == 0 || r.index >= r.count then 
            return (None,None,0)
        r.node match 
            case Some(node) => e = node.elements(r.index)
            case None =>
                bucket.getNodeElement(r.block,r.index) match
                    case Some(elem) => e = elem
                    case None => return (None,None,0)
        (Some(e.key),Some(e.value),e.flag)
    
    // 从指定id的node开始搜索key,并记录搜索路径
    private def seek(key:String,id:Int):Unit =
        var r:Record = null
        bucket.nodeOrBlock(id) match
            case (Some(n),_) =>
                r = new Record(Some(n),None,0)
            case (_,Some(b)) =>
                if b.btype!= branchType && b.btype!=leafType then 
                    throw new Exception(s"invalid page type:${b.btype}")
                r = new Record(None,Some(b),0)
            case (None,None) => throw new Exception(s"not found node or block for id:$id")
        
        stack ::= r 
        if r.isLeaf then 
            seekOnLeaf(key)
            return 
        r.node match
            case Some(node) => 
                seekOnNode(key,node)
            case None =>
                seekOnBlock(key,r.block)
    
    // search in leaf node.
    private def seekOnLeaf(key:String):Unit =
        var r = stack.last
        r.node match 
            case Some(node) =>
                val idx = node.elements.indexWhere((e:NodeElement) => e.key>=key) 
                r.index = idx
                stack = stack.init
                stack ::= r
            case None =>
                bucket.nodeElements(r.block) match
                    case Some(elems) => 
                        val idx = elems.indexWhere((e:NodeElement) => e.key>=key)
                        r.index = idx
                        stack = stack.init
                        stack ::= r
                    case None => return None
    
    // search in branch node.
    private def seekOnNode(key:String,node:Node):Unit =
        var hit:Boolean = false
        var idx = node.elements.indexWhere((e:NodeElement)=> e.key>=key)
        if idx>=0 && idx<node.elements.length && node.elements(idx).key == key then
            hit = true 
        if !hit && idx>0 then 
            idx-=1
        else if idx<0 then 
            idx = node.elements.length-1 // TODO ???
        var r =stack.last
        r.index = idx 
        stack = stack.init
        stack ::= r
        seek(key,node.elements(idx).child)
    
    // search in branch block.
    private def seekOnBlock(key:String,block:Option[Block]):Unit =
        bucket.nodeElements(block) match
            case None => return None
            case Some(elems) =>
                var hit:Boolean = false
                var idx:Int = elems.indexWhere((e:NodeElement) => e.key>=key)
                if idx>=0 && idx<elems.length && elems(idx).key == key then
                    hit = true 
                if !hit && idx>0 then 
                    idx-=1
                else if idx<0 then
                    idx = elems.length - 1
                var r= stack.last
                r.index = idx 
                stack = stack.init
                stack ::= r
                seek(key,elems(idx).child)
                
    // return current stack top node.
    private[platdb] def node():Option[Node] = 
        if stack.length == 0 then return None 
        var r= stack.last
        
        r.node match 
            case Some(node) =>
                if r.isLeaf then 
                    return r.node 
            case None => return None
        
        var n:Node = null
        r = stack.head
        r.node match 
            case Some(node) => n = node 
            case None =>
                r.block match
                    case None => return None
                    case Some(bk) =>
                        bucket.getNodeByBlock(Try(bk)) match
                            case Some(node) => 
                                r.node = Some(node)
                                n = node 
                            case None => return None 
        // top-down search from root.
        breakable(
            for r <- stack.tail do 
                bucket.getNodeChild(n,r.index) match
                    case Some(node) => n = node 
                    case None =>
                        n = null
                        break()
        )
        if n!=null then
            Some(n)
        else
            None
            

                

        

        
    

