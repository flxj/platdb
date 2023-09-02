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

// TODO private
class bucketIter(private var bucket:Bucket) extends BucketIterator:
    private var stack:List[Record] = _
    // 将迭代器移动到第一个元素上，并返回key-value值
    def first():(Option[String],Option[String]) = 
        if bucket.closed then return (None,None) // TODO: bucket已经关闭的情况下，考虑直接抛出异常
        stack = List[Record]()
        
        val (n,b) = bucket.nodeOrBlock(bucket.bkv.root)
        stack::=new Record(n,b,0)
        moveToFirst()
        
        val r = stack.last
        if r.count == 0 then // 如果定位到一个空节点，则需要移动到下一个节点
            moveToNext()
        
        current() match
            case (None,_,_) => return (None,None)
            case (k,v,f) =>
                if f == bucketType then 
                    return (k,None)
                return (k,v)
        
    // 移动到当前stack顶部元素所指向的子树的最右叶子节点
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
    // 移动到下一个叶子节点处，与next方法不同的一点是不会返回节点的值
    private def moveToNext():Unit = 
        if stack.length == 0 then return
        var r = stack.last 
        while r.count == 0 || r.index == r.count do // 退栈
            stack = stack.init
            if stack.length == 0 then 
                return 
            r = stack.last
        r.index = r.index+1 // 移动index到下一个位置
        stack = stack.init
        stack::=r

        moveToFirst()

    // iter移动到下一个元素位置，并返回该节点key-value, 如果该元素是个嵌套子bucket,那么value为None
    def next():(Option[String],Option[String]) = 
        if bucket.closed then return (None,None)
        moveToNext()
        current() match
            case (None,_,_) => return (None,None)
            case (k,v,f) =>
                if f == bucketType then 
                    return (k,None)
                return (k,v)

    // 判断是否还有下一个元素
    def hasNext(): Boolean =
        if bucket.closed || stack.length == 0 then 
            return false 
        
        var tmpStack = List[Record]()
        tmpStack :::=stack
        while tmpStack.length > 0 do 
            val r = tmpStack.last
            if r.index < r.count then // 不论是叶节点还是分支节点，只要index还没有遍历完所有元素，就说明还有下一个
                return true 
            tmpStack = tmpStack.init
        false 

    // iter移动到最后一个元素位置，并返回该元素的key-vaklue,如果该元素是个嵌套子bucket,那么value为None
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
    // iter移动到前一个元素位置，并返回该元素的key-vaklue,如果该元素是个嵌套子bucket,那么value为None
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
            if r.index > 0 then // 不论是叶节点还是分支节点，只要index还没有逆序遍历完所有元素，就说明还有前一个
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
                val (n,b) = bucket.nodeOrBlock(child)  // pageId等于0或1的位置是为meta预留的
                var r = new Record(n,b,0)
                r.index = r.count -1 
                stack ::= r
            
    private def moveToPrev():Unit =
        if stack.length == 0 then return
        var r = stack.last 
        while r.count == 0 || r.index == 0 do // 退栈
            stack = stack.init
            if stack.length == 0 then 
                return 
            r = stack.last
        r.index = r.index-1 // 移动index到前一个位置(前一个子树)
        stack = stack.init
        stack ::= r  
        
        moveToLast() // 移动到当前子树的最大节点处
     
    // 尝试将游标移动到key对应的元素上，并返回元素的值
    // 如果key不存在，则返回下一个元素（如果下一个元素为空，则返回空值）
    // 如果key是嵌套bucket，那末也返回None
    def find(key:String):(Option[String],Option[String]) = 
        if bucket.closed then return (None,None)
        stack = List[Record]()
        seek(key,bucket.bkv.root)
        
        val r = stack.last
        if r.index >= r.count then // 一直定位到了当前叶子节点的最大值处，需要移动到下一个位置处
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
    
    // 在叶节点上搜索
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
    
    // 在分支节点上搜索
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
    
    // 在分支block上搜索
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
                
    // 返回当前栈顶节点
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
        // 自根节点向下搜索
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
            

                

        

        
    

