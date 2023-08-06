package platdb

//case class KVPair(val key:String,val value:String)

trait BucketIterator extends Iterator[(Option[String],Option[String])]:
    def first():(Option[String],Option[String]) 
    def last():(Option[String],Option[String]) 
    def prev():(Option[String],Option[String]) 
    def find(key:String):(Option[String],Option[String])
    def next():(Option[String],Option[String])
    def hasNext():Boolean
    def hasPrev():Boolean 

class Record(var node:Option[Node],var block:Option[Block],var index:Int):
    def isLeaf:Boolean
    def isBranch:Boolean
    def count:Int 

class bucketIter(private var bucket:Bucket) extends BucketIterator:
    private var stack:List[Record] = _
    // 将迭代器移动到第一个元素上，并返回key-value值
    def first():(Option[String],Option[String]) = 
        if bucket.closed then return (None,None) // TODO: bucket已经关闭的情况下，考虑直接抛出异常
        stack = List[Record]()
        
        (n,b) := bucket.nodeOrBlock(bucket.bkv.root)
        stack = stack::Record(n,b,0)
        moveToFirst()
        
        r:= satack.last
        if r.count == 0 then // 如果定位到一个空节点，则需要移动到下一个节点
            moveToNext()
        
        current() match
            case (None,_,_) => return (None,None)
            case (k,v,f) =>
                if f == bucktType then 
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
                    child = node.elements[r.index].child
                case None =>
                    bucket.getNodeElement(r.block,r.index) match
                        case Some(e) => child = e.child 
                        case None => return 
            if child>1 then 
                (n,b) := bucket.nodeOrBlock(child) 
                stack = stack::Record(n,b,0)
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
        stack = stack.init::r 

        moveToFirst()

    // iter移动到下一个元素位置，并返回该节点key-value, 如果该元素是个嵌套子bucket,那么value为None
    def next():(Option[String],Option[String]) = 
        if bucket.closed then return (None,None)
        moveToNext()
        current() match
            case (None,_,_) => return (None,None)
            case (k,v,f) =>
                if f == bucktType then 
                    return (k,None)
                return (k,v)

    // 判断是否还有下一个元素
    def hasNext(): Boolean =
        if bucket.closed || stack.length == 0 then 
            return false 
        
        tmpStack:= List[Record]()
        tmpStack = tmpStack:::stack
        while tmpStack.length > 0 do 
            r:= tmpStack.last
            if r.index < r.count then // 不论是叶节点还是分支节点，只要index还没有遍历完所有元素，就说明还有下一个
                return true 
            tmpStack = tmpStack.init
        false 

    // iter移动到最后一个元素位置，并返回该元素的key-vaklue,如果该元素是个嵌套子bucket,那么value为None
    def last():(Option[String],Option[String])  = 
        if bucket.closed then return (None,None)
        stack = List[Record]()
        (n,b) := bucket.nodeOrBlock(bucket.bkv.root)
        r:= Record(n,b,0)
        r.index = r.count-1
        moveToLast()

        r:= stack.last
        if r.count == 0 then 
            moveToPrev()
        
        current() match
            case (None,_,_) => return (None,None)
            case (k,v,f) =>
                if f == bucktType then 
                    return (k,None)
                return (k,v)
    // iter移动到前一个元素位置，并返回该元素的key-vaklue,如果该元素是个嵌套子bucket,那么value为None
    def prev():(Option[String],Option[String]) = 
        if bucket.closed then return (None,None) 
        moveToPrev()
        current() match
            case (None,_,_) => return (None,None)
            case (k,v,f) =>
                if f == bucktType then 
                    return (k,None)
                return (k,v)

    def hasPrev():Boolean = 
        if bucket.closed || stack.length == 0 then 
            return false 
        
        tmpStack:= List[Record]()
        tmpStack = tmpStack:::stack
        while tmpStack.length > 0 do 
            r:= tmpStack.last
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
                    child = node.elements[r.index].child
                case None =>
                    bucket.getNodeElement(r.block,r.index) match
                        case Some(e) => child = e.child 
                        case None => return 
            if child>1 then 
                (n,b) := bucket.nodeOrBlock(child)  // blockId等于0或1的位置是为meta预留的
                r = Record(n,b,0)
                r.index = r.count -1 
                stack = stack::r
            
    private def moveToPrev():Unit =
        if stack.length == 0 then return
        var r = stack.last 
        while r.count == 0 || r.index == 0 do // 退栈
            stack = stack.init
            if stack.length == 0 then 
                return 
            r = stack.last
        r.index = r.index-1 // 移动index到前一个位置(前一个子树)
        stack = stack.init::r 
        
        moveToLast() // 移动到当前子树的最大节点处
     
    // 尝试将游标移动到key对应的元素上，并返回元素的值
    // 如果key不存在，则返回下一个元素（如果下一个元素为空，则返回空值）
    // 如果key是嵌套bucket，那末也返回None
    def find(key:String):(Option[String],Option[String]) = 
        if bucket.closed() then (None,None)
        stack = List[Record]()
        seek(key,bucket.bkv.root)
        
        r:= stack.last
        if r.index >= r.count then // 一直定位到了当前叶子节点的最大值处，需要移动到下一个位置处
            moveToNext()
        
        current() match 
            case (None,_,_) => return (None,None)
            case (Some(k),v,f) =>
                if f == bucketType then 
                    return (Some(k),None)
                return (Some(k),v)

    private[platdb] def search(key:String):(Option[String],Option[String],Int) =
        if bucket.closed() then (None,None,0)
        stack = List[Record]()
        seek(key,bucket.bkv.root)
        r:=stack.last
        if r.index >= r.count then 
            moveToNext()
        current() 

    // current
    private def current():(Option[String],Option[String],Int) = 
        r:= stack.last
        var e:NodeElement
        if r.count() == 0 || r.index >= r.count() then 
            return (None,None,0)
        r.node match 
            case Some(node) => e = node.elements[r.index]
            case None =>
                bucket.getNodeElement(r.block,e.index) match
                    case Some(elem) => e = elem
                    case None => return (None,None,0)
        (Some(e.key),Some(e.value),e.flag)
    
    // 从指定id的node开始搜索key,并记录搜索路径
    private def seek(key:String,id:Int):Unit =
        var r:Record = _
        bucket.nodeOrBlock(id) match
            case (Some(n),None) =>
                r = new Record(Some(n),None,0)
            case (None,Some(b)) =>
                if b.btype!= branchType && b.btype!=leafType then 
                    throw new Exception(s"invalid page type:${b.btype}")
                r = new Record(None,Some(b),0)
            case (None,None) => return   
        
        stack = stack::r 
        if r.isLeaf() then 
            seekOnLeaf(key)
            return 
        r.node match
            case Some(node) => 
                seekOnNode(key,node)
            case None =>
                seekOnBlock(key,r.block)
    
    // 在叶节点上搜索
    private def seekOnLeaf(key:String):Uint =
        r:= stack.last
        r.node match 
            case Some(node) =>
                idx:= r.node.elements.indexWhere((e:NodeElement) => e.key>=key) 
                r.index = idx
                stack = stack.init::r
            case None =>
                bucket.nodeElements(r.block) match
                    case Some(elems) => 
                        idx:= elems.indexWhere((e:NodeElement) => e.key>=key)
                        r.index = idx
                        stack = stack.init::r
                    case None => return 
    
    // 在分支节点上搜索
    private def seekOnNode(key:String,node:Node):Uint =
        var hit:Boolean
        idx:= node.elements.indexWhere((e:NodeElement)=> e.key>=key)
        if idx>=0 && idx<node.elements.length && node.elements[idx].key == key then
            hit = true 
        if !hit && idx>0 then 
            idx--
        else if idx<0 then 
            idx = node.elements.length-1 // TODO ???
        r:=stack.last
        r.index = idx 
        stack = stack.init::r
        seek(key,node.elements[idx].child)
    
    // 在分支block上搜索
    private def seekOnBlock(key:String,block:Option[Block]):Uint =
        block match
            case None => return
            case Some(bk) => 
                bucket.nodeElements(bk) match 
                    case None => return
                    case Some(elems) =>
                        var hit:Boolean
                        val idx:Int = elems.indexWhere((e:NodeElement) => e.key>=key)
                        if idx>=0 && idx<elems.length && elems[idx].key == key then
                            hit = true 
                        if !hit && idx>0 then 
                            idx--
                        else if idx<0 then
                            idx = elems.length - 1
                        r:= stack.last
                        r.index = idx 
                        stack = stack.init::r
                        seek(key,elems[idx].child)
    // 返回当前栈顶节点
    private[platdb] def node():Option[Node] = 
        if stack.length == 0 then return None 
        r:= stack.last
        
        r.node match 
            case Some(node) =>
                if r.isLeaf then 
                    return r.node 
            case None => _ 
        
        var n:Node = _ 
        r = stack.head
        r.node match 
            case Some(node) => n = noed 
            case None =>
                bucket.getNodeByBlock(r.block) match
                    case Some(node) => 
                        r.node = Some(node)
                        n = node 
                    case None => return None 
        
        for r <- stack.tail do 
            bucket.getNodeChild(n,r.index) match
                case Some(node) => n = node  
                case None => return None 
        Some(n)
            

                

        

        
    

